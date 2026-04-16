//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/operator.hpp"

#include <tenzir/chunk.hpp>
#include <tenzir/defaults.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::parquet {

namespace {

auto inject_tenzir_metadata(std::shared_ptr<arrow::RecordBatch> batch)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto needs_name = true;
  auto needs_stripping = false;
  auto metadata = batch->schema()->metadata();
  auto keys = metadata ? metadata->keys() : std::vector<std::string>{};
  auto values = metadata ? metadata->values() : std::vector<std::string>{};
  for (const auto& key : keys) {
    if (key == "TENZIR:name:0") {
      needs_name = false;
      continue;
    }
    if (not key.starts_with("TENZIR:")) {
      needs_stripping = true;
    }
  }
  if (not needs_name and not needs_stripping) {
    return batch;
  }
  if (needs_stripping) {
    auto kit = keys.begin();
    auto vit = values.begin();
    while (kit != keys.end()) {
      if (not kit->starts_with("TENZIR:")) {
        vit = values.erase(vit);
        kit = keys.erase(kit);
        continue;
      }
      ++kit;
      ++vit;
    }
  }
  if (needs_name) {
    keys.emplace_back("TENZIR:name:0");
    values.emplace_back("tenzir.parquet");
  }
  TENZIR_ASSERT(keys.size() == values.size());
  return batch->ReplaceSchemaMetadata(
    arrow::key_value_metadata(std::move(keys), std::move(values)));
}

struct ReadParquetArgs {};

class ReadParquet final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadParquet(ReadParquetArgs args) {
    TENZIR_UNUSED(args);
  }

  auto process(chunk_ptr input, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    // NOTE: The parquet format stores key decoding metadata in the file
    // footer. With plain streaming bytes, we cannot decode row groups before
    // seeing the footer, so we buffer and parse in `finalize()`. This also
    // means checkpointing is currently unsupported: restoring would require
    // persisting potentially huge buffered input and parser progress.
    if (not input or input->size() == 0) {
      co_return;
    }
    chunks_.push_back(std::move(input));
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    auto parquet_chunk = join_chunks(std::move(chunks_));
    if (parquet_chunk->size() == 0) {
      co_return FinalizeBehavior::done;
    }
    auto input_file = as_arrow_file(std::move(parquet_chunk));
    auto parquet_reader_properties
      = ::parquet::ReaderProperties(arrow_memory_pool());
    parquet_reader_properties.enable_buffered_stream();
    auto arrow_reader_properties = ::parquet::ArrowReaderProperties();
    arrow_reader_properties.set_batch_size(defaults::import::table_slice_size);
    std::unique_ptr<::parquet::arrow::FileReader> out_buffer;
    try {
      auto input_buffer = ::parquet::ParquetFileReader::Open(
        std::move(input_file), parquet_reader_properties);
      auto out_buffer_result = ::parquet::arrow::FileReader::Make(
        arrow_memory_pool(), std::move(input_buffer), arrow_reader_properties);
      if (not out_buffer_result.ok()) {
        diagnostic::error(
          "{}", out_buffer_result.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      out_buffer = std::move(out_buffer_result).MoveValueUnsafe();
    } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& err) {
      diagnostic::error("invalid or corrupted parquet file: {}", err.what())
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    auto rb_reader = out_buffer->GetRecordBatchReader();
    if (not rb_reader.ok()) {
      diagnostic::error("{}", rb_reader.status().ToStringWithoutContextLines())
        .note("failed create record batches from input data")
        .emit(ctx);
      co_return FinalizeBehavior::done;
    }
    for (auto maybe_batch : **rb_reader) {
      if (not maybe_batch.ok()) {
        diagnostic::error("{}",
                          maybe_batch.status().ToStringWithoutContextLines())
          .note("failed read record batch")
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      auto batch = maybe_batch.MoveValueUnsafe();
      /// We need to perform some cleanup, in case the parquet files were not
      /// written by us. Specifically we need to ensure that the slice has a
      /// name and that only metadata that are tenzir attributes exist.
      batch = inject_tenzir_metadata(std::move(batch));
      auto maybe_slice = table_slice::try_from(batch);
      if (not maybe_slice) {
        diagnostic::error("parquet file contains unsupported types")
          .note("{}", maybe_slice.error().message)
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      co_await push(std::move(*maybe_slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde&) -> void override {
    // Checkpointing this operator would require persisting the buffered parquet
    // bytes until the footer arrives, which can be arbitrarily large. Until we
    // have a seekable parquet path, we fail checkpoints explicitly.
    diagnostic::error("read_parquet does not support checkpoints yet").throw_();
  }

private:
  std::vector<chunk_ptr> chunks_;
};

class Plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadParquetArgs, ReadParquet>{};
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
