//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "parquet/chunked_buffer_output_stream.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/drain_bytes.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/io/file.h>
#include <arrow/table.h>
#include <caf/expected.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

namespace tenzir::plugins::parquet {

namespace {

auto parse_parquet(generator<chunk_ptr> input, operator_control_plane& ctrl)
  -> generator<table_slice> {
  auto parquet_chunk = chunk_ptr{};
  for (auto&& chunk : drain_bytes(std::move(input))) {
    if (not chunk) {
      co_yield {};
      continue;
    }
    TENZIR_ASSERT(not parquet_chunk);
    parquet_chunk = std::move(chunk);
  }
  auto input_file = as_arrow_file(std::move(parquet_chunk));
  auto parquet_reader_properties
    = ::parquet::ReaderProperties(arrow::default_memory_pool());
  parquet_reader_properties.enable_buffered_stream();
  std::unique_ptr<::parquet::arrow::FileReader> out_buffer;
  auto arrow_reader_properties = ::parquet::ArrowReaderProperties();
  arrow_reader_properties.set_batch_size(defaults::import::table_slice_size);
  try {
    auto input_buffer = ::parquet::ParquetFileReader::Open(
      std::move(input_file), parquet_reader_properties);
    ::arrow::Status arrow_file_reader_status
      = ::parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                           std::move(input_buffer),
                                           arrow_reader_properties,
                                           &out_buffer);
    if (! arrow_file_reader_status.ok()) {
      diagnostic::error("{}",
                        arrow_file_reader_status.ToStringWithoutContextLines())
        .emit(ctrl.diagnostics());
      co_return;
    }
  } catch (const ::parquet::ParquetInvalidOrCorruptedFileException& err) {
    diagnostic::error("invalid or corrupted parquet file: {}", err.what())
      .emit(ctrl.diagnostics());
    co_return;
  }
  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
  auto record_batch_reader_status
    = out_buffer->GetRecordBatchReader(&rb_reader);
  if (! record_batch_reader_status.ok()) {
    diagnostic::error("{}",
                      record_batch_reader_status.ToStringWithoutContextLines())
      .note("failed create record batches from input data")
      .emit(ctrl.diagnostics());
    co_return;
  }
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch :
       *rb_reader) {
    if (! maybe_batch.ok()) {
      diagnostic::error("{}",
                        maybe_batch.status().ToStringWithoutContextLines())
        .note("failed read record batch")
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield table_slice(maybe_batch.MoveValueUnsafe());
  }
}

class parquet_options {
public:
  std::optional<located<int64_t>> compression_level{};
  std::optional<located<std::string>> compression_type{};

  friend auto inspect(auto& f, parquet_options& x) -> bool {
    return f.object(x).fields(f.field("compression_level", x.compression_level),
                              f.field("compression_type", x.compression_type));
  }
};

class parquet_parser final : public plugin_parser {
public:
  parquet_parser() = default;

  auto name() const -> std::string override {
    return "parquet";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_parquet(std::move(input), ctrl);
  }

  friend auto inspect(auto& f, parquet_parser& x) -> bool {
    return f.object(x).fields();
  }
};

auto remove_empty_records(std::shared_ptr<arrow::Schema> schema,
                          diagnostic_handler& dh)
  -> std::shared_ptr<arrow::Schema> {
  auto impl = [](const auto& impl, std::shared_ptr<arrow::DataType> type,
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
    return type;
  };
  for (auto i = 0; i < schema->num_fields(); ++i) {
    auto field = schema->field(i);
    schema = check(schema->SetField(
      i, field->WithType(impl(impl, field->type(), dh, field->name()))));
  }
  return schema;
}

auto remove_empty_records(std::shared_ptr<arrow::RecordBatch> batch)
  -> std::shared_ptr<arrow::RecordBatch> {
  auto impl
    = [](const auto& impl,
         std::shared_ptr<arrow::Array> array) -> std::shared_ptr<arrow::Array> {
    TENZIR_ASSERT(array);
    if (const auto* list_array = try_as<arrow::ListArray>(array.get())) {
      auto values = impl(impl, list_array->values());
      return std::make_shared<arrow::ListArray>(arrow::list(values->type()),
                                                list_array->length(),
                                                list_array->value_offsets(),
                                                values);
    }
    if (const auto* struct_array = try_as<arrow::StructArray>(array.get())) {
      if (struct_array->num_fields() == 0) {
        return check(
          arrow::MakeArrayOfNull(arrow::null(), struct_array->length()));
      }
      auto arrays = struct_array->fields();
      auto fields = struct_array->struct_type()->fields();
      TENZIR_ASSERT(arrays.size() == fields.size());
      for (auto i = size_t{0}; i < arrays.size(); ++i) {
        arrays[i] = impl(impl, std::move(arrays[i]));
        fields[i] = fields[i]->WithType(arrays[i]->type());
      }
      return std::make_shared<arrow::StructArray>(
        arrow::struct_(fields), struct_array->length(), arrays);
    }
    return array;
  };
  for (auto i = 0; i < batch->num_columns(); ++i) {
    auto column = impl(impl, batch->column(i));
    batch = check(batch->SetColumn(
      i, batch->schema()->field(i)->WithType(column->type()), column));
  }
  return batch;
}

class parquet_printer final : public plugin_printer {
public:
  parquet_printer() = default;
  parquet_printer(parquet_options write_options)
    : options_{std::move(write_options)} {
  }
  auto name() const -> std::string override {
    return "parquet";
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    return parquet_printer_instance::make(ctrl, std::move(input_schema),
                                          options_);
  }

  auto allows_joining() const -> bool override {
    return false;
  }

  auto prints_utf8() const -> bool override {
    return false;
  }

  class parquet_printer_instance : public printer_instance {
  public:
    static auto make(operator_control_plane& ctrl, type input_schema,
                     const parquet_options& options)
      -> caf::expected<std::unique_ptr<printer_instance>> {
      auto arrow_writer_props
        = ::parquet::ArrowWriterProperties::Builder().store_schema()->build();
      auto parquet_writer_props_builder
        = ::parquet::WriterProperties::Builder();
      if (options.compression_type) {
        auto result_compression_type = arrow::util::Codec::GetCompressionType(
          options.compression_type->inner);
        if (! result_compression_type.ok()) {
          return diagnostic::error("{}", result_compression_type.status()
                                           .ToStringWithoutContextLines())
            .note("failed to parse compression type")
            .note("must be `brotli`, `gzip`, `snappy`, or `zstd`")
            .primary(options.compression_type->source)
            .to_error();
        }
        parquet_writer_props_builder.compression(
          result_compression_type.MoveValueUnsafe());
        if (options.compression_type->inner == "brotli"
            && (options.compression_level->inner < 1
                || options.compression_level->inner > 11)) {
          return diagnostic::error("")
            .note("invalid compression level")
            .note("must be a value between 1 and 11")
            .primary(options.compression_level->source)
            .to_error();
        }
        if (options.compression_type->inner == "gzip"
            && (options.compression_level->inner < 1
                || options.compression_level->inner > 9)) {
          return diagnostic::error("")
            .note("invalid compression level")
            .note("must be a value between 1 and 9")
            .primary(options.compression_level->source)
            .to_error();
        }
        if (options.compression_type->inner != "snappy") {
          parquet_writer_props_builder.compression_level(
            options.compression_level->inner);
        } else {
          diagnostic::warning("ignoring compression level option")
            .note("snappy does not accept `compression level`")
            .primary(options.compression_level
                       ? options.compression_level->source
                       : location::unknown)
            .primary(options.compression_type->source)
            .emit(ctrl.diagnostics());
        }
      } else {
        if (options.compression_level) {
          diagnostic::warning("ignoring compression level option")
            .note("has no effect without `compression type`")
            .primary(options.compression_level->source)
            .emit(ctrl.diagnostics());
        }
      }
      parquet_writer_props_builder.version(
        ::parquet::ParquetVersion::PARQUET_2_LATEST);
      auto parquet_writer_props = parquet_writer_props_builder.build();
      const auto schema = remove_empty_records(input_schema.to_arrow_schema(),
                                               ctrl.diagnostics());
      auto out_buffer = std::make_shared<chunked_buffer_output_stream>();
      auto file_result = ::parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), out_buffer,
        std::move(parquet_writer_props), std::move(arrow_writer_props));
      if (not file_result.ok()) {
        return diagnostic::error(
                 "failed to create parquet writer: {}",
                 file_result.status().ToStringWithoutContextLines())
          .to_error();
      }
      auto writer = file_result.MoveValueUnsafe();
      return std::make_unique<parquet_printer_instance>(ctrl,
                                                        std::move(input_schema),
                                                        std::move(out_buffer),
                                                        std::move(writer));
    }

    auto process(table_slice input) -> generator<chunk_ptr> override {
      // We need to force at least one co_yield, otherwise we will be stuck in
      // an infinite loop
      if (input.rows() == 0) {
        co_yield {};
        co_return;
      }
      auto [no_secrets, modified_fields] = replace_secrets(std::move(input));
      if (modified_fields.size() > 0) {
        diagnostic::warning("`secret` is serialized as text")
          .note("fields `{}` will be `\"***\"`",
                fmt::join(modified_fields, "`, `"))
          .emit(ctrl_.diagnostics());
      }
      auto record_batch = remove_empty_records(to_record_batch(no_secrets));
      auto record_batch_status = writer_->WriteRecordBatch(*record_batch);
      if (! record_batch_status.ok()) {
        diagnostic::error("{}",
                          record_batch_status.ToStringWithoutContextLines())
          .note("failed to write record batch")
          .emit(ctrl_.diagnostics());
        co_return;
      }
      co_yield out_buffer_->purge();
    }

    auto finish() -> generator<chunk_ptr> override {
      auto close_status = writer_->Close();
      if (! close_status.ok()) {
        diagnostic::error("{}", close_status.ToStringWithoutContextLines())
          .note("failed to write metadata and close")
          .emit(ctrl_.diagnostics());
        co_return;
      }
      co_yield out_buffer_->finish();
    }

    parquet_printer_instance(
      operator_control_plane& ctrl, type input_schema,
      std::shared_ptr<chunked_buffer_output_stream> out_buffer,
      std::unique_ptr<::parquet::arrow::FileWriter> writer)
      : ctrl_{ctrl},
        writer_{std::move(writer)},
        out_buffer_{std::move(out_buffer)},
        input_schema_{std::move(input_schema)} {
    }

  private:
    operator_control_plane& ctrl_;
    std::unique_ptr<::parquet::arrow::FileWriter> writer_;
    std::shared_ptr<chunked_buffer_output_stream> out_buffer_;
    type input_schema_;
  };

  friend auto inspect(auto& f, parquet_printer& x) -> bool {
    return f.object(x).fields(f.field("options", x.options_));
  }

private:
  parquet_options options_;
};
} // namespace
} // namespace tenzir::plugins::parquet
