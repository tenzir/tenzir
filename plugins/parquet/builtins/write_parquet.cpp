//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/operator.hpp"

#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::parquet {

namespace {

struct WriteParquetArgs {
  Option<located<int64_t>> compression_level;
  Option<located<std::string>> compression_type;
  Option<location> times_in_milliseconds;
};

class WriteParquet final : public Operator<table_slice, chunk_ptr> {
public:
  explicit WriteParquet(WriteParquetArgs args) : args_{std::move(args)} {
  }

  auto process(table_slice input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (failed_) {
      co_return;
    }
    if (not writer_) {
      if (not init_writer(input, ctx)) {
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
  auto init_writer(const table_slice& input, OpCtx& ctx) -> bool {
    auto arrow_writer_props
      = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
    auto parquet_writer_props_builder = ::parquet::WriterProperties::Builder();
    if (args_.compression_type) {
      auto result_compression_type = arrow::util::Codec::GetCompressionType(
        args_.compression_type->inner);
      if (not result_compression_type.ok()) {
        diagnostic::error(
          "{}", result_compression_type.status().ToStringWithoutContextLines())
          .note("failed to parse compression type")
          .note("must be `brotli`, `gzip`, `snappy`, or `zstd`")
          .primary(args_.compression_type->source)
          .emit(ctx);
        return false;
      }
      parquet_writer_props_builder.compression(
        result_compression_type.MoveValueUnsafe());
      if (args_.compression_level) {
        if (args_.compression_type->inner == "brotli"
            and (args_.compression_level->inner < 1
                 or args_.compression_level->inner > 11)) {
          diagnostic::error("invalid compression level")
            .note("must be a value between 1 and 11")
            .primary(args_.compression_level->source)
            .emit(ctx);
          return false;
        }
        if (args_.compression_type->inner == "gzip"
            and (args_.compression_level->inner < 1
                 or args_.compression_level->inner > 9)) {
          diagnostic::error("invalid compression level")
            .note("must be a value between 1 and 9")
            .primary(args_.compression_level->source)
            .emit(ctx);
          return false;
        }
        if (args_.compression_type->inner != "snappy") {
          parquet_writer_props_builder.compression_level(
            args_.compression_level->inner);
        } else {
          diagnostic::warning("ignoring compression level option")
            .note("snappy does not accept `compression level`")
            .primary(args_.compression_level->source)
            .primary(args_.compression_type->source)
            .emit(ctx);
        }
      }
    } else if (args_.compression_level) {
      diagnostic::warning("ignoring compression level option")
        .note("has no effect without `compression type`")
        .primary(args_.compression_level->source)
        .emit(ctx);
    }
    parquet_writer_props_builder.version(
      ::parquet::ParquetVersion::PARQUET_2_LATEST);
    auto parquet_writer_props = parquet_writer_props_builder.build();
    out_buffer_ = std::make_shared<chunked_buffer_output_stream>();
    const auto schema = remove_empty_records(
      input.schema().to_arrow_schema(),
      static_cast<bool>(args_.times_in_milliseconds), ctx.dh());
    auto file_result = ::parquet::arrow::FileWriter::Open(
      *schema, arrow_memory_pool(), out_buffer_, std::move(parquet_writer_props),
      std::move(arrow_writer_props));
    if (not file_result.ok()) {
      diagnostic::error("failed to create parquet writer: {}",
                        file_result.status().ToStringWithoutContextLines())
        .emit(ctx);
      return false;
    }
    writer_ = file_result.MoveValueUnsafe();
    return true;
  }

  WriteParquetArgs args_;
  std::shared_ptr<chunked_buffer_output_stream> out_buffer_;
  std::unique_ptr<::parquet::arrow::FileWriter> writer_;
  bool failed_ = false;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.write_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<WriteParquetArgs, WriteParquet>{};
    d.named("compression_level", &WriteParquetArgs::compression_level);
    d.named("compression_type", &WriteParquetArgs::compression_type);
    d.named("_times_in_milliseconds", &WriteParquetArgs::times_in_milliseconds);
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
