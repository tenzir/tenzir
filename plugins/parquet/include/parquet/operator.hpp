//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "parquet/chunked_buffer_output_stream.hpp"
#include "tenzir/arrow_memory_pool.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/drain_bytes.hpp>
#include <tenzir/fwd.hpp>
#include <tenzir/plugin.hpp>

#include <arrow/compute/cast.h>
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
    = ::parquet::ReaderProperties(arrow_memory_pool());
  parquet_reader_properties.enable_buffered_stream();
  std::unique_ptr<::parquet::arrow::FileReader> out_buffer;
  auto arrow_reader_properties = ::parquet::ArrowReaderProperties();
  arrow_reader_properties.set_batch_size(defaults::import::table_slice_size);
  try {
    auto input_buffer = ::parquet::ParquetFileReader::Open(
      std::move(input_file), parquet_reader_properties);
    ::arrow::Status arrow_file_reader_status
      = ::parquet::arrow::FileReader::Make(arrow_memory_pool(),
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
  auto rb_reader = out_buffer->GetRecordBatchReader();
  if (not rb_reader.ok()) {
    diagnostic::error("{}", rb_reader.status().ToStringWithoutContextLines())
      .note("failed create record batches from input data")
      .emit(ctrl.diagnostics());
    co_return;
  }
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch :
       **rb_reader) {
    if (! maybe_batch.ok()) {
      diagnostic::error("{}",
                        maybe_batch.status().ToStringWithoutContextLines())
        .note("failed read record batch")
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto maybe_slice = table_slice::try_from(maybe_batch.MoveValueUnsafe());
    if (not maybe_slice) {
      diagnostic::error("parquet file contains unsupported types")
        .note("{}", maybe_slice.error().message)
        .emit(ctrl.diagnostics());
      co_return;
    }
    co_yield std::move(*maybe_slice);
  }
}

class parquet_options {
public:
  std::optional<located<int64_t>> compression_level;
  std::optional<located<std::string>> compression_type;
  std::optional<location> times_in_milliseconds;

  friend auto inspect(auto& f, parquet_options& x) -> bool {
    return f.object(x).fields(f.field("compression_level", x.compression_level),
                              f.field("compression_type", x.compression_type),
                              f.field("times_in_milliseconds",
                                      x.times_in_milliseconds));
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
          return diagnostic::error("invalid compression level")
            .note("must be a value between 1 and 11")
            .primary(options.compression_level->source)
            .to_error();
        }
        if (options.compression_type->inner == "gzip"
            && (options.compression_level->inner < 1
                || options.compression_level->inner > 9)) {
          return diagnostic::error("invalid compression level")
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
      const auto schema
        = remove_empty_records(input_schema.to_arrow_schema(),
                               options.times_in_milliseconds.has_value(),
                               ctrl.diagnostics());
      auto out_buffer = std::make_shared<chunked_buffer_output_stream>();
      auto file_result = ::parquet::arrow::FileWriter::Open(
        *schema, arrow_memory_pool(), out_buffer,
        std::move(parquet_writer_props), std::move(arrow_writer_props));
      if (not file_result.ok()) {
        return diagnostic::error(
                 "failed to create parquet writer: {}",
                 file_result.status().ToStringWithoutContextLines())
          .to_error();
      }
      auto writer = file_result.MoveValueUnsafe();
      return std::make_unique<parquet_printer_instance>(
        ctrl, std::move(input_schema), std::move(out_buffer), std::move(writer),
        options.times_in_milliseconds.has_value());
    }

    auto process(table_slice input) -> generator<chunk_ptr> override {
      // We need to force at least one co_yield, otherwise we will be stuck in
      // an infinite loop
      if (input.rows() == 0) {
        co_yield {};
        co_return;
      }
      auto has_secrets = false;
      std::tie(has_secrets, input) = replace_secrets(std::move(input));
      if (has_secrets) {
        diagnostic::warning("`secret` is serialized as text")
          .note("fields will be `\"***\"`")
          .emit(ctrl_.diagnostics());
      }
      auto record_batch
        = remove_empty_records(to_record_batch(input), ms_timestamps_);
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
      std::unique_ptr<::parquet::arrow::FileWriter> writer, bool ms_timestamps)
      : ctrl_{ctrl},
        writer_{std::move(writer)},
        out_buffer_{std::move(out_buffer)},
        input_schema_{std::move(input_schema)},
        ms_timestamps_{ms_timestamps} {
    }

  private:
    operator_control_plane& ctrl_;
    std::unique_ptr<::parquet::arrow::FileWriter> writer_;
    std::shared_ptr<chunked_buffer_output_stream> out_buffer_;
    type input_schema_;
    bool ms_timestamps_;
  };

  friend auto inspect(auto& f, parquet_printer& x) -> bool {
    return f.object(x).fields(f.field("options", x.options_));
  }

private:
  parquet_options options_;
};
} // namespace
} // namespace tenzir::plugins::parquet
