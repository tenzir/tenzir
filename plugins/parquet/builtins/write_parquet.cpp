//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/chunked_buffer_output_stream.hpp"
#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/cast.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>
#include <caf/expected.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <limits>
#include <memory>

namespace tenzir::plugins::parquet {

namespace {

auto remove_empty_records(std::shared_ptr<arrow::Schema> schema, bool ms_times,
                          diagnostic_handler& dh)
  -> std::shared_ptr<arrow::Schema> {
  auto impl
    = [ms_times](const auto& impl, std::shared_ptr<arrow::DataType> type,
                 diagnostic_handler& dh,
                 std::string_view path) -> std::shared_ptr<arrow::DataType> {
    TENZIR_ASSERT(type);
    if (const auto* list_type = try_as<arrow::ListType>(type.get())) {
      return arrow::list(
        impl(impl, list_type->value_type(), dh, fmt::format("{}[]", path)));
    }
    if (const auto* struct_type = try_as<arrow::StructType>(type.get())) {
      if (struct_type->num_fields() == 0) {
        diagnostic::warning("replacing empty record with null at `{}`", path)
          .note("empty records are not supported in Apache Parquet")
          .emit(dh);
        return arrow::null();
      }
      auto fields = struct_type->fields();
      for (auto& field : fields) {
        field = field->WithType(impl(
          impl, field->type(), dh, fmt::format("{}.{}", path, field->name())));
      }
      return arrow::struct_(fields);
    }
    if (const auto* timestamp = try_as<arrow::TimestampType>(type.get());
        timestamp and ms_times) {
      return arrow::timestamp(arrow::TimeUnit::MILLI);
    }
    return type;
  };
  for (auto i = 0; i < schema->num_fields(); ++i) {
    auto field = schema->field(i);
    schema = check(schema->SetField(
      i, field->WithType(impl(impl, field->type(), dh, field->name()))));
  }
  return schema;
}

auto remove_empty_records(std::shared_ptr<arrow::RecordBatch> batch,
                          bool ms_timestamps)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto impl
    = [ms_timestamps](
        this const auto& self,
        std::shared_ptr<arrow::Array> array) -> std::shared_ptr<arrow::Array> {
    TENZIR_ASSERT(array);
    if (const auto* list_array = try_as<arrow::ListArray>(array.get())) {
      auto values = self(list_array->values());
      return std::make_shared<arrow::ListArray>(
        arrow::list(values->type()), list_array->length(),
        list_array->value_offsets(), values, array->null_bitmap(),
        array->data()->null_count, array->offset());
    }
    if (const auto* struct_array = try_as<arrow::StructArray>(array.get())) {
      if (struct_array->num_fields() == 0) {
        return check(arrow::MakeArrayOfNull(
          arrow::null(), struct_array->length(), tenzir::arrow_memory_pool()));
      }
      auto arrays = struct_array->fields();
      auto fields = struct_array->struct_type()->fields();
      TENZIR_ASSERT(arrays.size() == fields.size());
      for (auto i = size_t{0}; i < arrays.size(); ++i) {
        arrays[i] = self(std::move(arrays[i]));
        fields[i] = fields[i]->WithType(arrays[i]->type());
      }
      auto null_bitmap = array->null_bitmap();
      if (array->offset() != 0 and array->null_bitmap_data()) {
        null_bitmap = check(arrow::internal::CopyBitmap(
          arrow_memory_pool(), array->null_bitmap_data(), array->offset(),
          array->length()));
      }
      return std::make_shared<arrow::StructArray>(
        arrow::struct_(fields), struct_array->length(), arrays,
        std::move(null_bitmap), array->data()->null_count, 0);
    }
    if (const auto* timestamp = try_as<arrow::TimestampArray>(array.get());
        timestamp and ms_timestamps) {
      auto target = arrow::timestamp(arrow::TimeUnit::MILLI);
      auto result = check(arrow::compute::Cast(
        array, target, arrow::compute::CastOptions::Unsafe()));
      return result.make_array();
    }
    return array;
  };
  for (auto i = 0; i < batch->num_columns(); ++i) {
    auto column = impl(batch->column(i));
    batch = check(batch->SetColumn(
      i, batch->schema()->field(i)->WithType(column->type()), column));
  }
  return batch;
}

struct WriteParquetArgs {
  Option<located<int64_t>> compression_level;
  Option<located<std::string>> compression_type;
  Option<location> times_in_milliseconds;
  location operator_loc;
};

auto build_writer_props(WriteParquetArgs& args)
  -> std::shared_ptr<::parquet::WriterProperties> {
  auto parquet_writer_props_builder = ::parquet::WriterProperties::Builder();
  if (args.compression_type) {
    auto compression_type
      = arrow::util::Codec::GetCompressionType(args.compression_type->inner);
    // This should already be caught by validate_compression_arguments.
    TENZIR_ASSERT(compression_type.ok());
    parquet_writer_props_builder.compression(
      compression_type.MoveValueUnsafe());
    if (args.compression_level and args.compression_type->inner != "snappy") {
      parquet_writer_props_builder.compression_level(
        static_cast<int>(args.compression_level->inner));
    }
  }
  parquet_writer_props_builder.version(
    ::parquet::ParquetVersion::PARQUET_2_LATEST);
  return parquet_writer_props_builder.build();
}

class WriteParquet final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteParquet(WriteParquetArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    parquet_writer_props_ = build_writer_props(args_);
    co_return;
  }

  /// Checkpointing is not supported.
  ///
  /// Each `process()` call immediately flushes a row group to downstream via
  /// `purge()`. Those bytes are already in-flight and cannot be recalled. The
  /// Parquet footer (written only on `Close()`) references every prior row
  /// group by file offset, so restoring from a snapshot would either drop
  /// pre-checkpoint rows from the final file or produce an invalid concatenated
  /// Parquet stream. Checkpointing is not supported until we support
  /// seekable/appendable Parquet output.
  auto snapshot(Serde&) -> void override {
    TENZIR_TODO();
  }

  auto state() -> OperatorState override {
    return failed_ ? OperatorState::done : OperatorState::normal;
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    if (input.rows() == 0) {
      co_return;
    }
    auto input_schema = input.schema();
    if (input_schema_) {
      if (*input_schema_ != input_schema) {
        diagnostic::error("input schema changed while writing parquet")
          .note("all input slices to `write_parquet` must have the same schema")
          .note("first schema shape: `{}`", as<record_type>(*input_schema_))
          .note("current schema shape: `{}`", as<record_type>(input_schema))
          .primary(args_.operator_loc)
          .emit(ctx);
        failed_ = true;
        co_return;
      }
    } else {
      input_schema_ = std::move(input_schema);
    }
    TENZIR_ASSERT(input_schema_);
    TENZIR_ASSERT(parquet_writer_props_);
    if (not writer_) {
      if (not init_writer(ctx)) {
        failed_ = true;
        co_return;
      }
    }
    auto has_secrets = false;
    std::tie(has_secrets, input) = replace_secrets(std::move(input));
    if (has_secrets) {
      diagnostic::warning("`secret` is serialized as text")
        .note("fields will be `\"***\"`")
        .primary(args_.operator_loc)
        .emit(ctx);
    }
    auto record_batch = remove_empty_records(
      to_record_batch(input), static_cast<bool>(args_.times_in_milliseconds));
    auto record_batch_status = writer_->WriteRecordBatch(*record_batch);
    if (not record_batch_status.ok()) {
      diagnostic::error("{}", record_batch_status.ToStringWithoutContextLines())
        .note("failed to write record batch")
        .primary(args_.operator_loc)
        .emit(ctx);
      failed_ = true;
      co_return;
    }
    co_await push(out_buffer_->purge());
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (failed_ or not writer_) {
      co_return FinalizeBehavior::done;
    }
    auto close_status = writer_->Close();
    if (not close_status.ok()) {
      diagnostic::error("{}", close_status.ToStringWithoutContextLines())
        .note("failed to write metadata and close")
        .primary(args_.operator_loc)
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    co_await push(out_buffer_->finish());
    co_return FinalizeBehavior::done;
  }

private:
  auto init_writer(OpCtx& ctx) -> failure_or<void> {
    auto arrow_writer_props
      = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
    out_buffer_ = std::make_shared<chunked_buffer_output_stream>();
    const auto schema
      = remove_empty_records(input_schema_->to_arrow_schema(),
                             static_cast<bool>(args_.times_in_milliseconds),
                             ctx.dh());
    auto file_result
      = ::parquet::arrow::FileWriter::Open(*schema, arrow_memory_pool(),
                                           out_buffer_, parquet_writer_props_,
                                           std::move(arrow_writer_props));
    if (not file_result.ok()) {
      diagnostic::error("failed to create parquet writer: {}",
                        file_result.status().ToStringWithoutContextLines())
        .primary(args_.operator_loc)
        .emit(ctx);
      return failure::promise();
    }
    writer_ = file_result.MoveValueUnsafe();
    return {};
  }

  // --- args ---
  WriteParquetArgs args_;
  // --- transient ---
  // Note: Arc/Box cannot be used here. The parquet API boundary forces
  // std::shared_ptr and std::unique_ptr.
  std::shared_ptr<::parquet::WriterProperties> parquet_writer_props_;
  std::shared_ptr<chunked_buffer_output_stream> out_buffer_;
  std::unique_ptr<::parquet::arrow::FileWriter> writer_;
  // --- state ---
  Option<type> input_schema_;
  bool failed_ = false;
};

auto validate_args(const Option<located<std::string>>& type,
                   const Option<located<int64_t>>& level, DescribeCtx& ctx) {
  if (not type) {
    if (level) {
      diagnostic::warning("ignoring compression level option")
        .primary(level->source)
        .emit(ctx);
    }
    return;
  }
  auto result_compression_type
    = arrow::util::Codec::GetCompressionType(type->inner);
  if (not result_compression_type.ok()) {
    diagnostic::error(
      "{}", result_compression_type.status().ToStringWithoutContextLines())
      .note("failed to parse compression type")
      .note("must be `brotli`, `gzip`, `snappy`, or `zstd`")
      .primary(type->source)
      .emit(ctx);
  }
  if (level) {
    if (type->inner == "brotli" and (level->inner < 1 or level->inner > 11)) {
      diagnostic::error("invalid compression level")
        .note("must be a value between 1 and 11")
        .primary(level->source)
        .emit(ctx);
    }
    if (type->inner == "gzip" and (level->inner < 1 or level->inner > 9)) {
      diagnostic::error("invalid compression level")
        .note("must be a value between 1 and 9")
        .primary(level->source)
        .emit(ctx);
    }
    if (type->inner == "snappy") {
      diagnostic::warning("ignoring compression level option")
        .note("snappy does not accept `compression level`")
        .primary(level->source)
        .emit(ctx);
    }
    if (type->inner == "zstd" and (level->inner < 1 or level->inner > 22)) {
      diagnostic::error("invalid compression level")
        .note("must fit into a 32-bit signed integer")
        .primary(level->source)
        .emit(ctx);
    }
  }
}

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "write_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteParquetArgs, WriteParquet>{};
    auto compression_level
      = d.named("compression_level", &WriteParquetArgs::compression_level);
    auto compression_type
      = d.named("compression_type", &WriteParquetArgs::compression_type);
    d.named("_times_in_milliseconds", &WriteParquetArgs::times_in_milliseconds);
    d.operator_location(&WriteParquetArgs::operator_loc);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto level = ctx.get(compression_level);
      auto type = ctx.get(compression_type);
      validate_args(type, level, ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
