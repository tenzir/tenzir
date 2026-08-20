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
#include <tenzir/detail/enum.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/read_detection.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::parquet {

namespace {

TENZIR_ENUM(decimal_format, string, float_);

auto format_decimal_type(std::shared_ptr<arrow::DataType> type,
                         decimal_format format)
  -> std::shared_ptr<arrow::DataType> {
  switch (type->id()) {
    case arrow::Type::DECIMAL128:
      return format == decimal_format::string ? arrow::utf8()
                                              : arrow::float64();
    case arrow::Type::STRUCT: {
      auto fields = type->fields();
      auto changed = false;
      for (auto& field : fields) {
        auto field_type = format_decimal_type(field->type(), format);
        changed |= field_type != field->type();
        field = field->WithType(std::move(field_type));
      }
      return changed ? arrow::struct_(std::move(fields)) : std::move(type);
    }
    case arrow::Type::LIST: {
      auto list_type = std::static_pointer_cast<arrow::ListType>(type);
      auto value_type = format_decimal_type(list_type->value_type(), format);
      if (value_type == list_type->value_type()) {
        return type;
      }
      return arrow::list(
        list_type->value_field()->WithType(std::move(value_type)));
    }
    case arrow::Type::MAP: {
      if (format == decimal_format::float_) {
        return type;
      }
      auto map_type = std::static_pointer_cast<arrow::MapType>(type);
      auto key_type = format_decimal_type(map_type->key_type(), format);
      auto item_type = format_decimal_type(map_type->item_type(), format);
      if (key_type == map_type->key_type()
          and item_type == map_type->item_type()) {
        return type;
      }
      return std::make_shared<arrow::MapType>(
        map_type->key_field()->WithType(std::move(key_type)),
        map_type->item_field()->WithType(std::move(item_type)),
        map_type->keys_sorted());
    }
    default:
      return type;
  }
}

auto format_decimal_arrays(std::shared_ptr<arrow::RecordBatch> batch,
                           decimal_format format)
  -> arrow::Result<std::shared_ptr<arrow::RecordBatch>> {
  auto arrays = arrow::ArrayVector{};
  auto fields = arrow::FieldVector{};
  auto changed = false;
  arrays.reserve(batch->num_columns());
  fields.reserve(batch->num_columns());
  for (auto index = 0; index < batch->num_columns(); ++index) {
    auto array = batch->column(index);
    auto target_type = format_decimal_type(array->type(), format);
    if (target_type != array->type()) {
      ARROW_ASSIGN_OR_RAISE(
        auto result, arrow::compute::Cast(array, std::move(target_type)));
      array = result.make_array();
      changed = true;
    }
    arrays.push_back(array);
    fields.push_back(batch->schema()->field(index)->WithType(array->type()));
  }
  if (not changed) {
    return batch;
  }
  auto schema = std::make_shared<arrow::Schema>(std::move(fields),
                                                batch->schema()->endianness(),
                                                batch->schema()->metadata());
  return arrow::RecordBatch::Make(std::move(schema), batch->num_rows(),
                                  std::move(arrays));
}

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

struct ReadParquetArgs {
  Option<located<std::string>> decimal_format;
};

class ReadParquet final : public Operator<chunk_ptr, table_slice> {
public:
  explicit ReadParquet(ReadParquetArgs args)
    : decimal_format_{args.decimal_format ? from_string<decimal_format>(
                                              args.decimal_format->inner)
                                              .value_or(decimal_format::string)
                                          : decimal_format::string} {
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
      auto formatted_batch
        = format_decimal_arrays(std::move(batch), decimal_format_);
      if (not formatted_batch.ok()) {
        diagnostic::error("failed to format parquet decimals")
          .note("{}", formatted_batch.status().ToStringWithoutContextLines())
          .emit(ctx);
        co_return FinalizeBehavior::done;
      }
      batch = std::move(formatted_batch).MoveValueUnsafe();
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
  decimal_format decimal_format_ = decimal_format::string;
  std::vector<chunk_ptr> chunks_;
};

class Plugin final : public virtual ReadOperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.read_parquet";
  }

  auto describe() const -> Description override {
    auto d = Describer<ReadParquetArgs, ReadParquet>{};
    auto decimal_format_arg
      = d.named("decimal_format", &ReadParquetArgs::decimal_format);
    d.validate([decimal_format_arg](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(decimal_format_arg);
          value and not from_string<decimal_format>(value->inner)) {
        diagnostic::error("unsupported decimal format `{}`", value->inner)
          .primary(value->source)
          .note("supported decimal formats are `string` and `float`")
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto read_detection_candidates() const
    -> std::vector<read_detection_candidate> override {
    return {
      read_detection::candidate(
        "read_parquet", read_detection::specificity::magic,
        [](read_detection_input input) {
          return read_detection::magic_prefix(input, "PAR1");
        }),
    };
  }
};

} // namespace

} // namespace tenzir::plugins::parquet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::Plugin)
