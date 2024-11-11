//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/store.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/io/memory.h>
#include <arrow/ipc/feather.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/util/iterator.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/expected.hpp>

#include <queue>

namespace tenzir::plugins::feather {

namespace {

namespace store {

auto derive_import_time(const std::shared_ptr<arrow::Array>& time_col) {
  return value_at(time_type{}, *time_col, time_col->length() - 1);
}

/// Extract event column from record batch and transform into new record batch.
/// The record batch contains a message envelope with the actual event data
/// alongside Tenzir-related meta data (currently limited to the import time).
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
  if (auto status = builder->Reserve(rows); !status.ok()) {
    die(fmt::format("make time column failed: '{}'", status.ToString()));
  }
  for (int i = 0; i < rows; ++i) {
    auto status = builder->Append(v);
    TENZIR_ASSERT(status.ok());
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
           != 0) {
    return caf::make_error(ec::format_error, "not an Apache Feather v1 or "
                                             "Arrow IPC file");
  }
  auto open_reader_result
    = arrow::ipc::RecordBatchFileReader::Open(as_arrow_file(std::move(chunk)));
  if (!open_reader_result.ok()) {
    return caf::make_error(ec::format_error,
                           fmt::format("failed to open reader: {}",
                                       open_reader_result.status().ToString()));
  }
  auto reader = open_reader_result.MoveValueUnsafe();
  auto get_generator_result = reader->GetRecordBatchGenerator();
  if (!get_generator_result.ok()) {
    return caf::make_error(
      ec::format_error, fmt::format("failed to get batch generator: {}",
                                    get_generator_result.status().ToString()));
  }
  auto gen = get_generator_result.MoveValueUnsafe();
  return []([[maybe_unused]] auto reader,
            auto gen) -> generator<std::shared_ptr<arrow::RecordBatch>> {
    while (true) {
      auto next = gen();
      if (!next.is_finished()) {
        next.Wait();
      }
      TENZIR_ASSERT(next.is_finished());
      auto result = next.MoveResult().ValueOrDie();
      if (arrow::IsIterationEnd(result)) {
        co_return;
      }
      co_yield std::move(result);
    }
  }(std::move(reader), std::move(gen));
}

class passive_feather_store final : public passive_store {
  [[nodiscard]] caf::error load(chunk_ptr chunk) override {
    auto decode_result = decode_ipc_stream(std::move(chunk));
    if (!decode_result) {
      return caf::make_error(ec::format_error,
                             fmt::format("failed to load feather store: {}",
                                         decode_result.error()));
    }
    remaining_slices_generator_ = std::move(*decode_result);
    remaining_slices_iterator_ = remaining_slices_generator_.begin();
    return {};
  }

  [[nodiscard]] generator<table_slice> slices() const override {
    auto offset = id{};
    auto i = size_t{};
    while (true) {
      if (i < cached_slices_.size()) {
        TENZIR_ASSERT(offset == cached_slices_[i].offset());
      } else if (remaining_slices_iterator_
                 != remaining_slices_generator_.end()) {
        auto batch = std::move(*remaining_slices_iterator_);
        TENZIR_ASSERT(batch);
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
    if (cached_num_events_ == 0) {
      cached_num_events_ = rows(collect(slices()));
    }
    return cached_num_events_;
  }

  [[nodiscard]] type schema() const override {
    for (const auto& slice : slices()) {
      return slice.schema();
    }
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
  active_feather_store() = default;

  [[nodiscard]] caf::error add(std::vector<table_slice> new_slices) override {
    new_slices_.reserve(new_slices.size() + new_slices_.size());
    for (auto& slice : new_slices) {
      // The index already sets the correct offset for this slice, but in some
      // unit tests we test this component separately, causing incoming table
      // slices not to have an offset at all. We should fix the unit tests
      // properly, but that takes time we did not want to spend when migrating
      // to partition-local ids. -- DL
      if (slice.offset() == invalid_id) {
        slice.offset(num_events_);
      }
      TENZIR_ASSERT(slice.offset() == num_events_);
      num_events_ += slice.rows();
      num_new_events_ += slice.rows();
      new_slices_.push_back(std::move(slice));
    }
    while (num_new_events_ >= defaults::import::table_slice_size) {
      auto [lhs, rhs] = split(new_slices_, defaults::import::table_slice_size);
      rebatched_slices_.push_back(concatenate(std::move(lhs)));
      new_slices_ = std::move(rhs);
      num_new_events_ -= defaults::import::table_slice_size;
    }
    TENZIR_ASSERT(num_new_events_ == rows(new_slices_));
    return {};
  }

  [[nodiscard]] caf::expected<chunk_ptr> finish() override {
    if (num_new_events_ > 0) {
      rebatched_slices_.push_back(concatenate(std::exchange(new_slices_, {})));
    }
    auto record_batches = arrow::RecordBatchVector{};
    record_batches.reserve(rebatched_slices_.size());
    for (const auto& slice : rebatched_slices_) {
      record_batches.push_back(wrap_record_batch(slice));
    }
    const auto table = ::arrow::Table::FromRecordBatches(record_batches);
    if (!table.ok()) {
      return caf::make_error(ec::system_error, table.status().ToString());
    }
    auto output_stream = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto write_properties = arrow::ipc::feather::WriteProperties::Defaults();
    // TODO: Set write_properties.chunksize to the expected batch size
    write_properties.compression = arrow::Compression::ZSTD;
    const auto write_status = ::arrow::ipc::feather::WriteTable(
      *table.ValueUnsafe(), output_stream.get(), write_properties);
    if (!write_status.ok()) {
      return caf::make_error(ec::system_error, write_status.ToString());
    }
    auto buffer = output_stream->Finish();
    if (!buffer.ok()) {
      return caf::make_error(ec::system_error, buffer.status().ToString());
    }
    return chunk::make(buffer.MoveValueUnsafe());
  }

  [[nodiscard]] generator<table_slice> slices() const override {
    // We need to make a copy of the slices here because the slices_ vector
    // may get invalidated while we iterate over it.
    auto rebatched_slices = rebatched_slices_;
    auto new_slices = new_slices_;
    for (auto& slice : rebatched_slices) {
      co_yield std::move(slice);
    }
    for (auto& slice : new_slices) {
      co_yield std::move(slice);
    }
  }

  [[nodiscard]] uint64_t num_events() const override {
    return num_events_;
  }

private:
  std::vector<table_slice> rebatched_slices_ = {};
  std::vector<table_slice> new_slices_ = {};
  size_t num_new_events_ = {};
  size_t num_events_ = {};
};

} // namespace store

class callback_listener : public arrow::ipc::Listener {
public:
  callback_listener() = default;

  arrow::Status OnRecordBatchDecoded(
    std::shared_ptr<arrow::RecordBatch> record_batch) override {
    record_batch_buffer.push(std::move(record_batch));
    return arrow::Status::OK();
  }

  std::queue<std::shared_ptr<arrow::RecordBatch>> record_batch_buffer;
};

auto parse_feather(generator<chunk_ptr> input,
                   operator_control_plane& ctrl) -> generator<table_slice> {
  auto byte_reader = make_byte_reader(std::move(input));
  auto listener = std::make_shared<callback_listener>();
  auto stream_decoder = arrow::ipc::StreamDecoder(listener);
  auto truncated_bytes = size_t{0};
  auto decoded_once = false;
  while (true) {
    auto required_size
      = detail::narrow_cast<size_t>(stream_decoder.next_required_size());
    auto payload = byte_reader(required_size);
    if (!payload) {
      co_yield {};
      continue;
    }
    truncated_bytes += payload->size();
    if (payload->size() < required_size) {
      if (truncated_bytes != 0 and payload->size() != 0) {
        // Ideally this always would be just a warning, but the stream decoder
        // happily continues to consume invalid bytes. E.g., trying to read a
        // JSON file with this parser will just swallow all bytes, emitting this
        // one error at the very end. Not a single time does consuming a buffer
        // actually fail. We should probably look into limiting the memory usage
        // here, as the stream decoder will keep consumed-but-not-yet-converted
        // buffers in memory.
        diagnostic::warning("truncated {} trailing bytes", truncated_bytes)
          .severity(decoded_once ? severity::warning : severity::error)
          .emit(ctrl.diagnostics());
      }
      co_return;
    }
    auto decode_result
      = stream_decoder.Consume(as_arrow_buffer(std::move(payload)));
    if (!decode_result.ok()) {
      diagnostic::error("{}", decode_result.ToStringWithoutContextLines())
        .note("failed to decode the byte stream into a record batch")
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (!listener->record_batch_buffer.empty()) {
      decoded_once = true;
      truncated_bytes = 0;
      auto batch = listener->record_batch_buffer.front();
      listener->record_batch_buffer.pop();
      auto validate_status = batch->Validate();
      TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
      // We check whether the name metadatum from Tenzir's conversion to record
      // batches is still present. If it is not, then we stop parsing because we
      // cannot feasibly continue.
      // TODO: Implement a best-effort conversion for record batches coming from
      // other tools to Tenzir's supported subset and required metadata.
      const auto& metadata = batch->schema()->metadata();
      if (not metadata
          or std::find(metadata->keys().begin(), metadata->keys().end(),
                       "TENZIR:name:0")
               == metadata->keys().end()) {
        diagnostic::error("not implemented")
          .note("cannot convert Feather without Tenzir metadata")
          .emit(ctrl.diagnostics());
        co_return;
      }
      co_yield table_slice(batch);
    }
  }
}

auto print_feather(
  table_slice input, operator_control_plane& ctrl,
  const std::shared_ptr<arrow::ipc::RecordBatchWriter>& stream_writer,
  const std::shared_ptr<arrow::io::BufferOutputStream>& sink)
  -> generator<chunk_ptr> {
  auto batch = to_record_batch(input);
  auto validate_status = batch->Validate();
  TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
  auto stream_writer_status = stream_writer->WriteRecordBatch(*batch);
  if (!stream_writer_status.ok()) {
    diagnostic::error("{}", stream_writer_status.ToStringWithoutContextLines())
      .note("failed to write record batch")
      .emit(ctrl.diagnostics());
    co_return;
  }
  // We must finish the clear the buffer because the provided APIs do not offer
  // a scrape and rewrite on the allocated same memory.
  auto finished_buffer_result = sink->Finish();
  if (!finished_buffer_result.ok()) {
    diagnostic::error(
      "{}", finished_buffer_result.status().ToStringWithoutContextLines())
      .note("failed to finish stream")
      .emit(ctrl.diagnostics());
    co_return;
  }
  co_yield chunk::make(finished_buffer_result.MoveValueUnsafe());
  // The buffer is reinit with newly allocated memory because the API does not
  // offer a Reset that just clears the original data.
  auto reset_buffer_result = sink->Reset();
  if (!reset_buffer_result.ok()) {
    diagnostic::error("{}", reset_buffer_result.ToStringWithoutContextLines())
      .note("failed to reset stream")
      .emit(ctrl.diagnostics());
  }
}

class feather_options {
public:
  std::optional<located<int64_t>> compression_level{};
  std::optional<located<std::string>> compression_type{};
  std::optional<located<double>> min_space_savings{};

  friend auto inspect(auto& f, feather_options& x) -> bool {
    return f.object(x).fields(f.field("compression_level", x.compression_level),
                              f.field("compression_type", x.compression_type),
                              f.field("min_space_savings",
                                      x.min_space_savings));
  }
};

class feather_parser final : public plugin_parser {
public:
  feather_parser() = default;

  auto name() const -> std::string override {
    return "feather";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_feather(std::move(input), ctrl);
  }

  friend auto inspect(auto& f, feather_parser& x) -> bool {
    return f.object(x).fields();
  }
};

class feather_printer final : public plugin_printer {
public:
  feather_printer() = default;
  feather_printer(feather_options write_options)
    : options_{std::move(write_options)} {
  }

  auto name() const -> std::string override {
    // FIXME: Rename this and the file to just feather.
    return "feather";
  }

  auto instantiate([[maybe_unused]] type input_schema,
                   operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    auto sink = arrow::io::BufferOutputStream::Create();
    if (not sink.ok()) {
      return diagnostic::error("{}",
                               sink.status().ToStringWithoutContextLines())
        .note("failed to created BufferOutputStream")
        .to_error();
    }
    auto ipc_write_options = arrow::ipc::IpcWriteOptions::Defaults();
    if (!options_.compression_type) {
      if (options_.min_space_savings) {
        diagnostic::warning("ignoring min space savings option")
          .note("has no effect without `--compression-type`")
          .primary(options_.min_space_savings->source)
          .emit(ctrl.diagnostics());
      }
      if (options_.compression_level) {
        diagnostic::warning("ignoring compression level option")
          .note("has no effect without `--compression-type`")
          .primary(options_.compression_level->source)
          .emit(ctrl.diagnostics());
      }
    } else {
      auto result_compression_type = arrow::util::Codec::GetCompressionType(
        options_.compression_type->inner);
      if (!result_compression_type.ok()) {
        return diagnostic::error(
                 "{}",
                 result_compression_type.status().ToStringWithoutContextLines())
          .note("failed to parse compression type")
          .note("must be `lz4` or `zstd`")
          .primary(options_.compression_type->source)
          .to_error();
      }
      auto compression_level = options_.compression_level
                                 ? options_.compression_level->inner
                                 : arrow::util::kUseDefaultCompressionLevel;
      auto codec_result = arrow::util::Codec::Create(
        result_compression_type.MoveValueUnsafe(), compression_level);
      if (!codec_result.ok()) {
        return diagnostic::error(
                 "{}", codec_result.status().ToStringWithoutContextLines())
          .note("failed to create codec")
          .primary(options_.compression_type->source)
          .primary(options_.compression_level->source)
          .to_error();
      }
      ipc_write_options.codec = codec_result.MoveValueUnsafe();
      ipc_write_options.min_space_savings = options_.min_space_savings->inner;
    }
    const auto schema = input_schema.to_arrow_schema();
    auto stream_writer_result = arrow::ipc::MakeStreamWriter(
      sink.ValueUnsafe(), schema, ipc_write_options);
    if (!stream_writer_result.ok()) {
      return diagnostic::error(
               "{}",
               stream_writer_result.status().ToStringWithoutContextLines())
        .to_error();
    }
    auto stream_writer = stream_writer_result.MoveValueUnsafe();
    return printer_instance::make([&ctrl, sink = sink.MoveValueUnsafe(),
                                   stream_writer = std::move(stream_writer)](
                                    table_slice slice) -> generator<chunk_ptr> {
      return print_feather(std::move(slice), ctrl, stream_writer, sink);
    });
  }

  auto allows_joining() const -> bool override {
    return false;
  };

  auto prints_utf8() const -> bool override {
    return false;
  }

  friend auto inspect(auto& f, feather_printer& x) -> bool {
    return f.object(x).fields(f.field("options", x.options_));
  }

private:
  feather_options options_;
};

class plugin final : public virtual parser_plugin<feather_parser>,
                     public virtual printer_plugin<feather_printer>,
                     public virtual store_plugin {
  auto name() const -> std::string override {
    return "feather";
  }

  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"feather", "https://docs.tenzir.com/"
                                             "formats/feather"};
    parser.parse(p);
    return std::make_unique<feather_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto options = feather_options{};
    auto parser = argument_parser{"feather", "https://docs.tenzir.com/"
                                             "formats/feather"};
    parser.add("--compression-level", options.compression_level, "<level>");
    parser.add("--compression-type", options.compression_type, "<type>");
    parser.add("--min-space-savings", options.min_space_savings, "<rate>");
    parser.parse(p);
    return std::make_unique<feather_printer>(std::move(options));
  }

  [[nodiscard]] caf::expected<std::unique_ptr<passive_store>>
  make_passive_store() const override {
    return std::make_unique<store::passive_feather_store>();
  }

  [[nodiscard]] caf::expected<std::unique_ptr<active_store>>
  make_active_store() const override {
    return std::make_unique<store::active_feather_store>();
  }
};

class read_plugin final
  : public virtual operator_plugin2<parser_adapter<feather_parser>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<parser_adapter<feather_parser>>(feather_parser{});
  }
};

class write_plugin final
  : public virtual operator_plugin2<writer_adapter<feather_printer>> {
public:
  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto options = feather_options{};
    TRY(argument_parser2::operator_(name())
          .add("compression_level", options.compression_level)
          .add("compression_type", options.compression_type)
          .add("min_space_savings", options.min_space_savings)
          .parse(inv, ctx));
    return std::make_unique<writer_adapter<feather_printer>>(
      feather_printer{std::move(options)});
  }
};

} // namespace

} // namespace tenzir::plugins::feather

TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::read_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::write_plugin)
