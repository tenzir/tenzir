//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/operator.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"

#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <limits>
#include <memory>

namespace tenzir::plugins::parquet {

namespace {

struct WriteParquetArgs {
  Option<located<int64_t>> compression_level;
  Option<located<std::string>> compression_type;
  Option<location> times_in_milliseconds;
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
  /// Parquet stream. Until we support seekable/appendable Parquet output, we
  /// fail checkpoints explicitly rather than silently producing corrupt data.
  auto snapshot(Serde&) -> void override {
    diagnostic::error("write_parquet does not support checkpoints yet").throw_();
  }

  auto state() -> OperatorState override {
    return failed_ ? OperatorState::done : OperatorState::unspecified;
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
        .emit(ctx);
    }
    auto record_batch = remove_empty_records(
      to_record_batch(input), static_cast<bool>(args_.times_in_milliseconds));
    auto record_batch_status = writer_->WriteRecordBatch(*record_batch);
    if (not record_batch_status.ok()) {
      diagnostic::error("{}", record_batch_status.ToStringWithoutContextLines())
        .note("failed to write record batch")
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

auto validate_compression_arguments(const Option<located<std::string>>& type,
                                    const Option<located<int64_t>>& level,
                                    DescribeCtx& ctx) {
  if (type) {
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
      if (type->inner != "snappy") {
        if (level->inner < std::numeric_limits<int>::min()
            or level->inner > std::numeric_limits<int>::max()) {
          diagnostic::error("invalid compression level")
            .note("must fit into a 32-bit signed integer")
            .primary(level->source)
            .emit(ctx);
        }
      } else {
        diagnostic::warning("ignoring compression level option")
          .note("snappy does not accept `compression level`")
          .primary(level->source)
          .primary(type->source)
          .emit(ctx);
      }
    }
  } else if (level) {
    diagnostic::warning("ignoring compression level option")
      .note("has no effect without `compression type`")
      .primary(level->source)
      .emit(ctx);
  }
}

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteParquetArgs, WriteParquet>{};
    auto compression_level
      = d.named("compression_level", &WriteParquetArgs::compression_level);
    auto compression_type
      = d.named("compression_type", &WriteParquetArgs::compression_type);
    d.named("_times_in_milliseconds", &WriteParquetArgs::times_in_milliseconds);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto level = ctx.get(compression_level);
      auto type = ctx.get(compression_type);
      validate_compression_arguments(type, level, ctx);
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
