//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/box.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/collect.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/error.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/make_byte_reader.hpp>
#include <tenzir/operator_plugin.hpp>
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

auto has_location(diagnostic const& diag) -> bool {
  for (auto const& annotation : diag.annotations) {
    if (annotation.source != location::unknown) {
      return true;
    }
  }
  return false;
}

template <class DiagnosticBuilder, class DiagnosticHandler>
auto emit_with_location(DiagnosticBuilder&& diag, DiagnosticHandler& dh,
                        location operator_location) -> void {
  if (operator_location and not has_location(diag.inner())) {
    diag.inner().annotations.emplace_back(true, std::string{},
                                          operator_location);
  }
  std::forward<DiagnosticBuilder>(diag).emit(dh);
}

namespace store {

auto derive_import_time(const std::shared_ptr<arrow::Array>& time_col) {
  if (not time_col or time_col->length() == 0) {
    return time{};
  }
  auto const row = time_col->length() - 1;
  if (time_col->IsNull(row)) {
    return time{};
  }
  return *view_at<time_type>(*time_col, row);
}

/// Extract event column from record batch and transform into new record batch.
/// The record batch contains a message envelope with the actual event data
/// alongside Tenzir-related meta data (currently limited to the import time).
/// Message envelope is unwrapped and the metadata, attached to the to-level
/// schema the input record batch is copied to the newly created record batch.
auto unwrap_record_batch(const std::shared_ptr<arrow::RecordBatch>& rb)
  -> std::shared_ptr<arrow::RecordBatch> {
  const auto& event_col = as<arrow::StructArray>(*rb->GetColumnByName("event"));
  auto schema_metadata = rb->schema()->GetFieldByName("event")->metadata();
  auto event_schema
    = arrow::schema(event_col.type()->fields(), std::move(schema_metadata));
  return record_batch_from_struct_array(std::move(event_schema), event_col);
}

/// Create a constant column for the given import time with `rows` rows
auto make_import_time_col(const time& import_time, int64_t rows) {
  auto v = import_time.time_since_epoch().count();
  auto builder = time_type::make_arrow_builder(arrow_memory_pool());
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
      or std::memcmp(chunk->data(), arrow_magic_bytes.data(),
                     arrow_magic_bytes.size())
           != 0) {
    return caf::make_error(ec::format_error, "not an Apache Feather v1 or "
                                             "Arrow IPC file");
  }
  auto open_reader_result = arrow::ipc::RecordBatchFileReader::Open(
    as_arrow_file(std::move(chunk)), arrow_ipc_read_options());
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
      as_arrow_file(make_chunk_view()), arrow_ipc_read_options());
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
    auto table = ::arrow::Table::FromRecordBatches(record_batches);
    if (not table.ok()) {
      return caf::make_error(ec::system_error, table.status().ToString());
    }
    // Attach origin metadata to the table schema.
    {
      auto metadata = table.ValueUnsafe()->schema()->metadata()
                        ? table.ValueUnsafe()->schema()->metadata()->Copy()
                        : std::make_shared<arrow::KeyValueMetadata>();
      metadata->Append("TENZIR:store:origin", origin());
      table = table.ValueUnsafe()->ReplaceSchemaMetadata(std::move(metadata));
    }
    auto output_stream
      = check(arrow::io::BufferOutputStream::Create(4096, arrow_memory_pool()));
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
          and slice.rows() == defaults::import::table_slice_size) {
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
  auto stream_decoder
    = arrow::ipc::StreamDecoder(listener, arrow_ipc_read_options());
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
  auto reset_buffer_result = sink->Reset(1024, arrow_memory_pool());
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
    auto sink
      = arrow::io::BufferOutputStream::Create(4096, arrow_memory_pool());
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
          .note("must be `uncompressed`, `lz4`, or `zstd`")
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
          .primary(options_.compression_level
                     ? options_.compression_level->source
                     : location::unknown)
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

struct ReadFeatherArgs {
  location operator_location = location::unknown;
};

class ReadFeather final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadFeather(ReadFeatherArgs args)
    : args_{std::move(args)},
      listener_{std::make_shared<callback_listener>()},
      stream_decoder_{std::in_place, listener_, arrow_ipc_read_options()} {
  }

  auto process(chunk_ptr input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or not input or input->size() == 0) {
      co_return;
    }
    append(std::move(input));
    co_await parse_available(push, ctx.dh());
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    auto trailing_bytes = truncated_bytes_ + available();
    if (not done_ and trailing_bytes != 0) {
      emit_with_location(diagnostic::warning("truncated Feather input")
                           .note("discarded {} trailing bytes", trailing_bytes)
                           .severity(decoded_once_ ? severity::warning
                                                   : severity::error),
                         ctx.dh(), args_.operator_location);
    }
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto available() const -> size_t {
    TENZIR_ASSERT(buffer_);
    return buffer_->size() - offset_;
  }

  auto append(chunk_ptr input) -> void {
    TENZIR_ASSERT(buffer_);
    TENZIR_ASSERT(input);
    if (available() == 0) {
      buffer_ = std::move(input);
      offset_ = 0;
      return;
    }
    auto merged_buffer
      = std::make_unique<chunk::value_type[]>(available() + input->size());
    std::memcpy(merged_buffer.get(), buffer_->data() + offset_, available());
    std::memcpy(merged_buffer.get() + available(), input->data(),
                input->size());
    auto merged_buffer_view = std::span<const std::byte>{
      merged_buffer.get(), available() + input->size()};
    buffer_
      = chunk::make(merged_buffer_view,
                    [merged_buffer = std::move(merged_buffer)]() noexcept {
                      static_cast<void>(merged_buffer);
                    });
    offset_ = 0;
  }

  auto take(size_t required_size) -> chunk_ptr {
    if (available() < required_size) {
      return {};
    }
    auto result = buffer_->slice(offset_, required_size);
    offset_ += required_size;
    if (offset_ == buffer_->size()) {
      buffer_ = chunk::make_empty();
      offset_ = 0;
    }
    return result;
  }

  auto parse_available(Push<table_slice>& push, diagnostic_handler& dh)
    -> Task<void> {
    while (not done_) {
      auto required_size
        = detail::narrow_cast<size_t>(stream_decoder_->next_required_size());
      if (required_size == 0) {
        co_return;
      }
      auto payload = take(required_size);
      if (not payload) {
        co_return;
      }
      truncated_bytes_ += payload->size();
      auto decode_result = stream_decoder_->Consume(as_arrow_buffer(payload));
      if (not decode_result.ok()) {
        emit_with_location(
          diagnostic::error("failed to decode Feather input")
            .note("{}", decode_result.ToStringWithoutContextLines())
            .note("failed to decode the byte stream into a record batch"),
          dh, args_.operator_location);
        done_ = true;
        co_return;
      }
      if (stream_decoder_->next_required_size() == 0) {
        truncated_bytes_ = 0;
      }
      while (not listener_->record_batch_buffer.empty()) {
        decoded_once_ = true;
        truncated_bytes_ = 0;
        auto batch = listener_->record_batch_buffer.front();
        listener_->record_batch_buffer.pop();
        auto validate_status = batch->Validate();
        TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
        const auto& metadata = batch->schema()->metadata();
        if (not metadata
            or std::find(metadata->keys().begin(), metadata->keys().end(),
                         "TENZIR:name:0")
                 == metadata->keys().end()) {
          emit_with_location(
            diagnostic::error("missing Tenzir metadata in Feather input")
              .note("cannot convert Feather without the `TENZIR:name:0` "
                    "metadata entry"),
            dh, args_.operator_location);
          done_ = true;
          co_return;
        }
        co_await push(table_slice{std::move(batch)});
      }
    }
  }

  ReadFeatherArgs args_;
  chunk_ptr buffer_ = chunk::make_empty();
  size_t offset_ = 0;
  size_t truncated_bytes_ = 0;
  bool decoded_once_ = false;
  bool done_ = false;
  std::shared_ptr<callback_listener> listener_;
  Box<arrow::ipc::StreamDecoder> stream_decoder_;
};

struct WriteFeatherArgs {
  location operator_location = location::unknown;
  Option<located<int64_t>> compression_level;
  Option<located<std::string>> compression_type;
  Option<located<double>> min_space_savings;
};

template <class DiagnosticHandler>
auto make_feather_write_options(WriteFeatherArgs const& args,
                                DiagnosticHandler& dh)
  -> failure_or<arrow::ipc::IpcWriteOptions> {
  auto result = arrow::ipc::IpcWriteOptions::Defaults();
  if (not args.compression_type) {
    if (args.min_space_savings) {
      diagnostic::warning("ignoring min space savings option")
        .note("has no effect without `compression_type`")
        .primary(args.min_space_savings->source)
        .emit(dh);
    }
    if (args.compression_level) {
      diagnostic::warning("ignoring compression level option")
        .note("has no effect without `compression_type`")
        .primary(args.compression_level->source)
        .emit(dh);
    }
    return result;
  }
  if (args.min_space_savings
      and (args.min_space_savings->inner < 0.0
           or args.min_space_savings->inner > 1.0)) {
    diagnostic::error("min space savings must be between 0 and 1")
      .primary(args.min_space_savings->source)
      .emit(dh);
    return failure::promise();
  }
  auto const& compression_type = args.compression_type->inner;
  if (compression_type != "uncompressed" and compression_type != "lz4"
      and compression_type != "zstd") {
    auto parse_result
      = arrow::util::Codec::GetCompressionType(compression_type);
    if (not parse_result.ok()) {
      diagnostic::error("invalid Feather compression type")
        .note("{}", parse_result.status().ToStringWithoutContextLines())
        .note("must be `uncompressed`, `lz4`, or `zstd`")
        .primary(args.compression_type->source)
        .emit(dh);
    } else {
      diagnostic::error("unsupported Feather compression type `{}`",
                        compression_type)
        .note("must be `uncompressed`, `lz4`, or `zstd`")
        .primary(args.compression_type->source)
        .emit(dh);
    }
    return failure::promise();
  }
  auto compression_type_result
    = arrow::util::Codec::GetCompressionType(compression_type);
  TENZIR_ASSERT(compression_type_result.ok());
  auto compression_level = args.compression_level
                             ? args.compression_level->inner
                             : arrow::util::kUseDefaultCompressionLevel;
  auto codec_result = arrow::util::Codec::Create(
    compression_type_result.MoveValueUnsafe(), compression_level);
  if (not codec_result.ok()) {
    auto diagnostic
      = diagnostic::error("failed to create Feather codec")
          .note("{}", codec_result.status().ToStringWithoutContextLines())
          .primary(args.compression_type->source);
    if (args.compression_level) {
      diagnostic
        = std::move(diagnostic).primary(args.compression_level->source);
    }
    std::move(diagnostic).emit(dh);
    return failure::promise();
  }
  result.codec = codec_result.MoveValueUnsafe();
  if (args.min_space_savings) {
    result.min_space_savings = args.min_space_savings->inner;
  }
  return result;
}

class WriteFeather final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteFeather(WriteFeatherArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or input.rows() == 0) {
      co_return;
    }
    if (not writer_) {
      if (not initialize(input.schema(), ctx.dh())) {
        done_ = true;
        co_return;
      }
    } else if (*schema_ != input.schema()) {
      emit_with_location(
        diagnostic::error("`feather` writer does not support heterogeneous "
                          "outputs")
          .note("cannot initialize for schema `{}` after schema `{}`",
                input.schema(), *schema_),
        ctx.dh(), args_.operator_location);
      done_ = true;
      co_return;
    }
    auto has_secrets = false;
    std::tie(has_secrets, input) = replace_secrets(std::move(input));
    if (has_secrets) {
      emit_with_location(diagnostic::warning("`secret` is serialized as text")
                           .note("fields will be `\"***\"`"),
                         ctx.dh(), args_.operator_location);
    }
    auto batch = to_record_batch(input);
    auto validate_status = batch->Validate();
    TENZIR_ASSERT(validate_status.ok(), validate_status.ToString().c_str());
    auto write_status = writer_->WriteRecordBatch(*batch);
    if (not write_status.ok()) {
      emit_with_location(
        diagnostic::error("failed to write Feather record batch")
          .note("{}", write_status.ToStringWithoutContextLines()),
        ctx.dh(), args_.operator_location);
      done_ = true;
      co_return;
    }
    co_await emit_buffer(push, ctx.dh(), true);
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (done_ or not writer_) {
      co_return FinalizeBehavior::done;
    }
    auto close_status = writer_->Close();
    if (not close_status.ok()) {
      emit_with_location(
        diagnostic::error("failed to close Feather output stream")
          .note("{}", close_status.ToStringWithoutContextLines()),
        ctx.dh(), args_.operator_location);
      done_ = true;
      co_return FinalizeBehavior::done;
    }
    co_await emit_buffer(push, ctx.dh(), false);
    co_return FinalizeBehavior::done;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  auto initialize(type schema, diagnostic_handler& dh) -> bool {
    auto sink_result
      = arrow::io::BufferOutputStream::Create(4096, arrow_memory_pool());
    if (not sink_result.ok()) {
      emit_with_location(
        diagnostic::error("failed to create Feather buffer output stream")
          .note("{}", sink_result.status().ToStringWithoutContextLines()),
        dh, args_.operator_location);
      return false;
    }
    auto write_options = make_feather_write_options(args_, dh);
    if (not write_options) {
      return false;
    }
    auto stream_writer_result = arrow::ipc::MakeStreamWriter(
      sink_result.ValueUnsafe(), schema.to_arrow_schema(), *write_options);
    if (not stream_writer_result.ok()) {
      emit_with_location(
        diagnostic::error("failed to create Feather stream writer")
          .note("{}",
                stream_writer_result.status().ToStringWithoutContextLines()),
        dh, args_.operator_location);
      return false;
    }
    schema_ = std::move(schema);
    sink_ = sink_result.MoveValueUnsafe();
    writer_ = stream_writer_result.MoveValueUnsafe();
    return true;
  }

  auto emit_buffer(Push<chunk_ptr>& push, diagnostic_handler& dh, bool reset)
    -> Task<void> {
    auto buffer_result = sink_->Finish();
    if (not buffer_result.ok()) {
      emit_with_location(
        diagnostic::error("failed to finish Feather output stream")
          .note("{}", buffer_result.status().ToStringWithoutContextLines()),
        dh, args_.operator_location);
      done_ = true;
      co_return;
    }
    auto buffer = buffer_result.MoveValueUnsafe();
    if (buffer->size() > 0) {
      co_await push(chunk::make(std::move(buffer)));
    }
    if (not reset) {
      sink_.reset();
      co_return;
    }
    auto reset_result = sink_->Reset(1024, arrow_memory_pool());
    if (not reset_result.ok()) {
      emit_with_location(
        diagnostic::error("failed to reset Feather output stream")
          .note("{}", reset_result.ToStringWithoutContextLines()),
        dh, args_.operator_location);
      done_ = true;
    }
  }

  WriteFeatherArgs args_;
  Option<type> schema_;
  std::shared_ptr<arrow::io::BufferOutputStream> sink_;
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer_;
  bool done_ = false;
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
  : public virtual operator_plugin2<parser_adapter<feather_parser>>,
    public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<ReadFeatherArgs, ReadFeather>{};
    d.operator_location(&ReadFeatherArgs::operator_location);
    return d.without_optimize();
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(argument_parser2::operator_(name()).parse(inv, ctx));
    return std::make_unique<parser_adapter<feather_parser>>(feather_parser{});
  }

  auto read_properties() const -> read_properties_t override {
    return {.extensions = {"feather", "arrow"}};
  }
};

class write_plugin final
  : public virtual operator_plugin2<writer_adapter<feather_printer>>,
    public virtual OperatorPlugin {
public:
  auto describe() const -> Description override {
    auto d = Describer<WriteFeatherArgs, WriteFeather>{};
    d.operator_location(&WriteFeatherArgs::operator_location);
    auto compression_level
      = d.named("compression_level", &WriteFeatherArgs::compression_level);
    auto compression_type
      = d.named("compression_type", &WriteFeatherArgs::compression_type);
    auto min_space_savings
      = d.named("min_space_savings", &WriteFeatherArgs::min_space_savings);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto args = WriteFeatherArgs{};
      args.compression_level = ctx.get(compression_level);
      args.compression_type = ctx.get(compression_type);
      args.min_space_savings = ctx.get(min_space_savings);
      std::ignore = make_feather_write_options(args, ctx);
      return {};
    });
    return d.without_optimize();
  }

  auto make(operator_factory_invocation inv, session ctx) const
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
