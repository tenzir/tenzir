//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
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

#include <optional>
#include <queue>
#include <string_view>

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
auto unwrap_record_batch(const std::shared_ptr<arrow::RecordBatch>& rb)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto event_col = rb->GetColumnByName("event");
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_rb = check(arrow::RecordBatch::FromStructArray(event_col));
  return event_rb->ReplaceSchemaMetadata(schema_metadata);
}

/// Create a constant column for the given import time with `rows` rows
auto make_import_time_col(const time& import_time, int64_t rows) {
  auto v = import_time.time_since_epoch().count();
  auto builder = time_type::make_arrow_builder(arrow::default_memory_pool());
  check(builder->Reserve(rows));
  for (int i = 0; i < rows; ++i) {
    auto status = builder->Append(v);
    TENZIR_ASSERT(status.ok());
  }
  return finish(*builder);
}

/// Wrap a record batch into an event envelope containing the event data
/// as a nested struct alongside metadata as separate columns, containing
/// the `import_time`.
auto wrap_record_batch(const table_slice& slice)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto rb = to_record_batch(slice);
  auto event_array = check(rb->ToStructArray());
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
  if (not open_reader_result.ok()) {
    return caf::make_error(ec::format_error,
                           fmt::format("failed to open reader: {}",
                                       open_reader_result.status().ToString()));
  }
  auto reader = open_reader_result.MoveValueUnsafe();
  auto get_generator_result = reader->GetRecordBatchGenerator();
  if (not get_generator_result.ok()) {
    return caf::make_error(
      ec::format_error, fmt::format("failed to get batch generator: {}",
                                    get_generator_result.status().ToString()));
  }
  auto gen = get_generator_result.MoveValueUnsafe();
  return
    []([[maybe_unused]] auto reader,
       decltype(gen) gen) -> generator<std::shared_ptr<arrow::RecordBatch>> {
      while (true) {
        auto next = gen();
        if (not next.is_finished()) {
          // TODO: We block the CAF worker thread here. Presumably we are okay
          // with this since work is happening in an Arrow worker thread.
          next.Wait();
        }
        TENZIR_ASSERT(next.is_finished());
        auto result = check(next.MoveResult());
        if (arrow::IsIterationEnd(result)) {
          co_return;
        }
        co_yield std::move(result);
      }
    }(std::move(reader), std::move(gen));
}

class passive_feather_store final : public passive_store {
  [[nodiscard]] auto load(chunk_ptr chunk) -> caf::error override {
    TENZIR_ASSERT(chunk);
    if (auto decode_result = decode_ipc_stream(chunk->slice(0, chunk->size()));
        not decode_result) {
      return caf::make_error(ec::format_error,
                             fmt::format("failed to load feather store: {}",
                                         decode_result.error()));
    }
    chunk_ = std::move(chunk);
    schema_.reset();
    num_events_.reset();
    return {};
  }

  [[nodiscard]] auto slices() const -> generator<table_slice> override {
    if (not chunk_) {
      co_return;
    }
    auto decode_result = decode_ipc_stream(make_chunk_view());
    if (not decode_result) {
      TENZIR_ASSERT(false, "failed to decode feather store after load");
      co_return;
    }
    auto batches = std::move(*decode_result);
    auto offset = id{};
    auto schema = schema_;
    for (auto it = batches.begin(); it != batches.end(); ++it) {
      auto batch = std::move(*it);
      TENZIR_ASSERT(batch);
      auto import_time_column = batch->GetColumnByName("import_time");
      auto slice = schema ? table_slice{unwrap_record_batch(batch), *schema}
                          : table_slice{unwrap_record_batch(batch)};
      if (not schema) {
        schema = slice.schema();
        schema_ = schema;
      }
      slice.offset(offset);
      slice.import_time(derive_import_time(import_time_column));
      offset += slice.rows();
      co_yield std::move(slice);
    }
  }

  [[nodiscard]] auto num_events() const -> uint64_t override {
    if (not num_events_) {
      num_events_ = count_rows();
    }
    return *num_events_;
  }

  [[nodiscard]] auto schema() const -> type override {
    if (schema_) {
      return *schema_;
    }
    for (const auto& slice : slices()) {
      return slice.schema();
    }
    TENZIR_ASSERT(false, "store must not be empty");
  }

private:
  [[nodiscard]] auto make_chunk_view() const -> chunk_ptr {
    TENZIR_ASSERT(chunk_);
    return chunk_->slice(0, chunk_->size());
  }

  [[nodiscard]] auto count_rows() const -> uint64_t {
    auto reader_result = arrow::ipc::RecordBatchFileReader::Open(
      as_arrow_file(make_chunk_view()));
    check(reader_result.status());
    auto reader = reader_result.MoveValueUnsafe();
    auto rows = check(reader->CountRows());
    return detail::narrow_cast<uint64_t>(rows);
  }

  chunk_ptr chunk_;
  mutable std::optional<uint64_t> num_events_;
  mutable std::optional<type> schema_;
};

class active_feather_store final : public active_store {
public:
  active_feather_store(int64_t compression_level)
    : compression_level_{compression_level} {
  }

  [[nodiscard]] auto add(std::vector<table_slice> new_slices)
    -> caf::error override {
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
      // Track non-optimally sized batches and rows for rebatching
      if (rebatch_batches_ > 0
          or slice.rows() != defaults::import::table_slice_size) {
        rebatch_batches_ += 1;
        rebatch_rows_ += slice.rows();
      }
      slices_.push_back(std::move(slice));
    }
    // Rebatch when we have too many small slices or enough rows to form a
    // complete slice to avoid memory overhead and doing it later at once.
    auto max_rebatch_batches = size_t{512};
    if (rebatch_batches_ > max_rebatch_batches
        or rebatch_rows_ >= defaults::import::table_slice_size) {
      rebatch();
    }
    return {};
  }

  [[nodiscard]] auto finish() -> caf::expected<chunk_ptr> override {
    rebatch();
    auto record_batches = arrow::RecordBatchVector{};
    record_batches.reserve(slices_.size());
    for (const auto& slice : slices_) {
      record_batches.push_back(wrap_record_batch(slice));
    }
    const auto table = ::arrow::Table::FromRecordBatches(record_batches);
    if (not table.ok()) {
      return caf::make_error(ec::system_error, table.status().ToString());
    }
    auto output_stream = check(arrow::io::BufferOutputStream::Create());
    auto write_properties = arrow::ipc::feather::WriteProperties::Defaults();
    write_properties.compression = arrow::Compression::ZSTD;
    write_properties.compression_level = compression_level_;
    write_properties.chunksize = defaults::import::table_slice_size;
    const auto write_status = ::arrow::ipc::feather::WriteTable(
      *table.ValueUnsafe(), output_stream.get(), write_properties);
    if (not write_status.ok()) {
      return caf::make_error(ec::system_error, write_status.ToString());
    }
    auto buffer = output_stream->Finish();
    if (not buffer.ok()) {
      return caf::make_error(ec::system_error, buffer.status().ToString());
    }
    return chunk::make(buffer.MoveValueUnsafe());
  }

  [[nodiscard]] auto slices() const -> generator<table_slice> override {
    rebatch();
    for (auto& slice : slices_) {
      co_yield slice;
    }
  }

  [[nodiscard]] auto num_events() const -> uint64_t override {
    return num_events_;
  }

private:
  void rebatch() const {
    auto result = std::vector<table_slice>{};
    auto pending = std::vector<table_slice>{};
    // Note: We move the slices of down below in order to directly release their
    // memory once they are rebatched.
    for (auto& slice : slices_) {
      // If current slice is exactly target size and we have no pending slices,
      // keep it as-is.
      if (pending.empty()
          && slice.rows() == defaults::import::table_slice_size) {
        result.push_back(std::move(slice));
        continue;
      }
      // Add to pending accumulator
      pending.push_back(std::move(slice));
      // If we've accumulated enough rows, process the batch
      while (rows(pending) >= defaults::import::table_slice_size) {
        auto [lhs, rhs]
          = split(std::move(pending), defaults::import::table_slice_size);
        result.push_back(concatenate(std::move(lhs)));
        pending = std::move(rhs);
      }
    }
    // Handle any remaining pending slices.
    if (pending.empty()) {
      rebatch_batches_ = 0;
      rebatch_rows_ = 0;
    } else {
      result.push_back(concatenate(std::move(pending)));
      rebatch_batches_ = 1;
      rebatch_rows_ = result.back().rows();
    }
    slices_ = std::move(result);
  }

  int64_t compression_level_;
  size_t num_events_ = {};
  mutable std::vector<table_slice> slices_;
  mutable size_t rebatch_batches_ = {};
  mutable size_t rebatch_rows_ = {};
};

} // namespace store

class callback_listener : public arrow::ipc::Listener {
public:
  callback_listener() = default;

  auto OnRecordBatchDecoded(std::shared_ptr<arrow::RecordBatch> record_batch)
    -> arrow::Status override {
    record_batch_buffer.push(std::move(record_batch));
    return arrow::Status::OK();
  }

  std::queue<std::shared_ptr<arrow::RecordBatch>> record_batch_buffer;
};

auto parse_feather(generator<chunk_ptr> input, operator_control_plane& ctrl)
  -> generator<table_slice> {
  auto byte_reader = make_byte_reader(std::move(input));
  auto listener = std::make_shared<callback_listener>();
  auto stream_decoder = arrow::ipc::StreamDecoder(listener);
  auto truncated_bytes = size_t{0};
  auto decoded_once = false;
  while (true) {
    auto required_size
      = detail::narrow_cast<size_t>(stream_decoder.next_required_size());
    auto payload = byte_reader(required_size);
    if (not payload) {
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
    if (not decode_result.ok()) {
      diagnostic::error("{}", decode_result.ToStringWithoutContextLines())
        .note("failed to decode the byte stream into a record batch")
        .emit(ctrl.diagnostics());
      co_return;
    }
    while (not listener->record_batch_buffer.empty()) {
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
  auto has_secrets = false;
  std::tie(has_secrets, input) = replace_secrets(std::move(input));
  if (has_secrets) {
    diagnostic::warning("`secret` is serialized as text")
      .note("fields will be `\"***\"`")
      .emit(ctrl.diagnostics());
  }
  auto batch = to_record_batch(input);
  auto validate_status = batch->Validate();
  TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
  auto stream_writer_status = stream_writer->WriteRecordBatch(*batch);
  if (not stream_writer_status.ok()) {
    diagnostic::error("{}", stream_writer_status.ToStringWithoutContextLines())
      .note("failed to write record batch")
      .emit(ctrl.diagnostics());
    co_return;
  }
  // We must finish the clear the buffer because the provided APIs do not offer
  // a scrape and rewrite on the allocated same memory.
  auto finished_buffer_result = sink->Finish();
  if (not finished_buffer_result.ok()) {
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
  if (not reset_buffer_result.ok()) {
    diagnostic::error("{}", reset_buffer_result.ToStringWithoutContextLines())
      .note("failed to reset stream")
      .emit(ctrl.diagnostics());
  }
}

class feather_options {
public:
  std::optional<located<int64_t>> compression_level;
  std::optional<located<std::string>> compression_type;
  std::optional<located<double>> min_space_savings;

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
    if (not options_.compression_type) {
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
      if (not result_compression_type.ok()) {
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
      if (not codec_result.ok()) {
        return diagnostic::error(
                 "{}", codec_result.status().ToStringWithoutContextLines())
          .note("failed to create codec")
          .primary(options_.compression_type->source)
          .primary(options_.compression_level->source)
          .to_error();
      }
      ipc_write_options.codec = codec_result.MoveValueUnsafe();
      if (options_.min_space_savings) {
        ipc_write_options.min_space_savings = options_.min_space_savings->inner;
      }
    }
    const auto schema = input_schema.to_arrow_schema();
    auto stream_writer_result = arrow::ipc::MakeStreamWriter(
      sink.ValueUnsafe(), schema, ipc_write_options);
    if (not stream_writer_result.ok()) {
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
  auto initialize(const record& plugin_config, const record& global_config)
    -> caf::error override {
    TENZIR_UNUSED(plugin_config);
    const auto default_compression_level = int64_t{check(
      arrow::util::Codec::DefaultCompressionLevel(arrow::Compression::ZSTD))};
    compression_level_ = get_or(global_config, "tenzir.zstd-compression-level",
                                default_compression_level);
    return {};
  }

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

  [[nodiscard]] auto make_passive_store() const
    -> caf::expected<std::unique_ptr<passive_store>> override {
    return std::make_unique<store::passive_feather_store>();
  }

  [[nodiscard]] auto make_active_store() const
    -> caf::expected<std::unique_ptr<active_store>> override {
    return std::make_unique<store::active_feather_store>(compression_level_);
  }

private:
  int64_t compression_level_ = 0;
};

class read_plugin final
  : public virtual operator_plugin2<parser_adapter<feather_parser>> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<parser_adapter<feather_parser>>(feather_parser{});
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"feather", "arrow"}};
  }
};

class write_plugin final
  : public virtual operator_plugin2<writer_adapter<feather_printer>> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto options = feather_options{};
    TRY(argument_parser2::operator_(name())
          .named("compression_level", options.compression_level)
          .named("compression_type", options.compression_type)
          .named("min_space_savings", options.min_space_savings)
          .parse(inv, ctx));
    return std::make_unique<writer_adapter<feather_printer>>(
      feather_printer{std::move(options)});
  }

  auto write_properties() const -> write_properties_t override {
    return {.extensions = {"feather", "arrow"}};
  }
};

} // namespace

} // namespace tenzir::plugins::feather

TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::read_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::feather::write_plugin)
