//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/arrow_table_slice.hpp>
#include <vast/chunk.hpp>
#include <vast/collect.hpp>
#include <vast/concept/convertible/data.hpp>
#include <vast/data.hpp>
#include <vast/detail/narrow.hpp>
#include <vast/error.hpp>
#include <vast/fwd.hpp>
#include <vast/generator.hpp>
#include <vast/plugin.hpp>
#include <vast/store.hpp>
#include <vast/table_slice.hpp>

#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>
#include <arrow/util/key_value_metadata.h>

namespace vast::plugins::feather {

namespace {

/// Configuration for the Feather plugin.
struct configuration {
  int64_t zstd_compression_level{
    arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
      .ValueOrDie()};

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f.apply(x.zstd_compression_level);
  }

  static const record_type& schema() noexcept {
    static auto result = record_type{
      {"zstd-compression-level", int64_type{}},
    };
    return result;
  }
};

auto derive_import_time(const std::shared_ptr<arrow::Array>& time_col) {
  return value_at(time_type{}, *time_col, time_col->length() - 1);
}

/// Extract event column from record batch and transform into new record batch.
/// The record batch contains a message envelope with the actual event data
/// alongside VAST-related meta data (currently limited to the import time).
/// Message envelope is unwrapped and the metadata, attached to the to-level
/// schema the input record batch is copied to the newly created record batch.
std::shared_ptr<arrow::RecordBatch>
unwrap_record_batch(const std::shared_ptr<arrow::RecordBatch>& rb) {
  auto event_col = rb->GetColumnByName("event");
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_rb = arrow::RecordBatch::FromStructArray(event_col).ValueOrDie();
  return event_rb->ReplaceSchemaMetadata(schema_metadata);
}

/// Create a constant column for the given import time with `rows` rows
auto make_import_time_col(const time& import_time, int64_t rows) {
  auto v = import_time.time_since_epoch().count();
  auto builder = time_type::make_arrow_builder(arrow::default_memory_pool());
  if (auto status = builder->Reserve(rows); !status.ok())
    die(fmt::format("make time column failed: '{}'", status.ToString()));
  for (int i = 0; i < rows; ++i) {
    auto status = builder->Append(v);
    VAST_ASSERT(status.ok());
  }
  return builder->Finish().ValueOrDie();
}

/// Wrap a record batch into an event envelope containing the event data
/// as a nested struct alongside metadata as separate columns, containing
/// the `import_time`.
auto wrap_record_batch(const table_slice& slice)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto rb = to_record_batch(slice);
  auto event_array = rb->ToStructArray().ValueOrDie();
  auto time_col = make_import_time_col(slice.import_time(), rb->num_rows());
  auto schema = arrow::schema(
    {arrow::field("import_time", time_type::to_arrow_type()),
     arrow::field("event", event_array->type(), rb->schema()->metadata())});
  auto new_rb
    = arrow::RecordBatch::Make(schema, rb->num_rows(), {time_col, event_array});
  return new_rb;
}

/// Decode an Arrow IPC stream incrementally.
auto decode_ipc_stream(chunk_ptr chunk)
  -> caf::expected<generator<std::shared_ptr<arrow::RecordBatch>>> {
  // See arrow::ipc::internal::kArrowMagicBytes in
  // arrow/ipc/metadata_internal.h.
  static constexpr auto arrow_magic_bytes = std::string_view{"ARROW1"};
  if (chunk->size() < arrow_magic_bytes.length()
      || std::memcmp(chunk->data(), arrow_magic_bytes.data(),
                     arrow_magic_bytes.size())
           != 0)
    return caf::make_error(ec::format_error, "not an Apache Feather v1 or "
                                             "Arrow IPC file");
  auto open_reader_result
    = arrow::ipc::RecordBatchFileReader::Open(as_arrow_file(std::move(chunk)));
  if (!open_reader_result.ok())
    return caf::make_error(ec::format_error,
                           fmt::format("failed to open reader: {}",
                                       open_reader_result.status().ToString()));
  auto reader = open_reader_result.MoveValueUnsafe();
  auto get_generator_result = reader->GetRecordBatchGenerator();
  if (!get_generator_result.ok())
    return caf::make_error(
      ec::format_error, fmt::format("failed to get batch generator: {}",
                                    get_generator_result.status().ToString()));
  auto gen = get_generator_result.MoveValueUnsafe();
  return []([[maybe_unused]] auto reader,
            auto gen) -> generator<std::shared_ptr<arrow::RecordBatch>> {
    while (true) {
      auto next = gen();
      if (!next.is_finished())
        next.Wait();
      VAST_ASSERT(next.is_finished());
      auto result = next.MoveResult().ValueOrDie();
      if (arrow::IsIterationEnd(result))
        co_return;
      co_yield std::move(result);
    }
  }(std::move(reader), std::move(gen));
}

class passive_feather_store final : public passive_store {
  [[nodiscard]] caf::error load(chunk_ptr chunk) override {
    auto decode_result = decode_ipc_stream(std::move(chunk));
    if (!decode_result)
      return caf::make_error(ec::format_error,
                             fmt::format("failed to load feather store: {}",
                                         decode_result.error()));
    remaining_slices_generator_ = std::move(*decode_result);
    remaining_slices_iterator_ = remaining_slices_generator_.begin();
    return {};
  }

  [[nodiscard]] generator<table_slice> slices() const override {
    auto offset = id{};
    auto i = size_t{};
    while (true) {
      if (i < cached_slices_.size()) {
        VAST_ASSERT(offset == cached_slices_[i].offset());
      } else if (remaining_slices_iterator_
                 != remaining_slices_generator_.end()) {
        auto batch = std::move(*remaining_slices_iterator_);
        VAST_ASSERT(batch);
        ++remaining_slices_iterator_;
        auto import_time_column = batch->GetColumnByName("import_time");
        auto slice = cached_slices_.empty()
                       ? table_slice{unwrap_record_batch(batch)}
                       : table_slice{unwrap_record_batch(batch),
                                     cached_slices_[0].schema()};
        slice.offset(offset);
        slice.import_time(derive_import_time(import_time_column));
        cached_slices_.push_back(std::move(slice));
      } else {
        co_return;
      }
      co_yield cached_slices_[i];
      offset += cached_slices_[i].rows();
      ++i;
    }
  }

  [[nodiscard]] uint64_t num_events() const override {
    if (cached_num_events_ == 0)
      cached_num_events_ = rows(collect(slices()));
    return cached_num_events_;
  }

  [[nodiscard]] type schema() const override {
    for (const auto& slice : slices())
      return slice.schema();
    die("store must not be empty");
  }

private:
  generator<std::shared_ptr<arrow::RecordBatch>> remaining_slices_generator_
    = {};
  mutable generator<std::shared_ptr<arrow::RecordBatch>>::iterator
    remaining_slices_iterator_
    = {};
  mutable uint64_t cached_num_events_ = {};
  mutable std::vector<table_slice> cached_slices_ = {};
};

class active_feather_store final : public active_store {
public:
  explicit active_feather_store(const configuration& config)
    : feather_config_(config) {
  }

  [[nodiscard]] caf::error add(std::vector<table_slice> new_slices) override {
    slices_.reserve(new_slices.size() + slices_.size());
    for (auto& slice : new_slices) {
      // The index already sets the correct offset for this slice, but in some
      // unit tests we test this component separately, causing incoming table
      // slices not to have an offset at all. We should fix the unit tests
      // properly, but that takes time we did not want to spend when migrating
      // to partition-local ids. -- DL
      if (slice.offset() == invalid_id)
        slice.offset(num_events_);
      VAST_ASSERT(slice.offset() == num_events_);
      num_events_ += slice.rows();
      slices_.push_back(std::move(slice));
    }
    return {};
  }

  [[nodiscard]] caf::expected<chunk_ptr> finish() override {
    auto record_batches = arrow::RecordBatchVector{};
    record_batches.reserve(slices_.size());
    for (const auto& slice : slices_)
      record_batches.push_back(wrap_record_batch(slice));
    const auto table = ::arrow::Table::FromRecordBatches(record_batches);
    if (!table.ok())
      return caf::make_error(ec::system_error, table.status().ToString());
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto write_properties = arrow::ipc::feather::WriteProperties::Defaults();
    // TODO: Set write_properties.chunksize to the expected batch size
    write_properties.compression = arrow::Compression::ZSTD;
    write_properties.compression_level
      = detail::narrow<int>(feather_config_.zstd_compression_level);
    const auto write_status = ::arrow::ipc::feather::WriteTable(
      *table.ValueUnsafe(), output_stream.get(), write_properties);
    if (!write_status.ok())
      return caf::make_error(ec::system_error, write_status.ToString());
    auto buffer = output_stream->Finish();
    if (!buffer.ok())
      return caf::make_error(ec::system_error, buffer.status().ToString());
    return chunk::make(buffer.MoveValueUnsafe());
  }

  [[nodiscard]] generator<table_slice> slices() const override {
    // We need to make a copy of the slices here because the slices_ vector
    // may get invalidated while we iterate over it.
    auto slices = slices_;
    for (auto& slice : slices)
      co_yield std::move(slice);
  }

  [[nodiscard]] uint64_t num_events() const override {
    return num_events_;
  }

private:
  std::vector<table_slice> slices_ = {};
  configuration feather_config_ = {};
  size_t num_events_ = {};
};

class plugin final : public virtual store_plugin {
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    const auto default_compression_level
      = arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD)
          .ValueOrDie();
    auto level = try_get_or(global_config, "vast.zstd-compression-level",
                            default_compression_level);
    if (!level) {
      return std::move(level.error());
    }
    zstd_compression_level_ = *level;
    return convert(plugin_config, feather_config_);
  }

  [[nodiscard]] std::string name() const override {
    return "feather";
  }

  [[nodiscard]] caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const override {
    return std::make_unique<passive_feather_store>();
  }

  [[nodiscard]] caf::expected<std::unique_ptr<active_store>>
  make_active_store() const override {
    return std::make_unique<active_feather_store>(
      configuration{zstd_compression_level_});
  }

private:
  int zstd_compression_level_ = {};
  configuration feather_config_ = {};
};

} // namespace

} // namespace vast::plugins::feather

VAST_REGISTER_PLUGIN(vast::plugins::feather::plugin)
