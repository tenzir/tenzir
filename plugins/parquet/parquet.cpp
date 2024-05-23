//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "parquet/chunked_buffer_output_stream.hpp"

#include <tenzir/argument_parser.hpp>
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
auto parse_parquet(generator<chunk_ptr> input, operator_control_plane& ctrl,
                   located<select_optimization> selection)
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
    if (!arrow_file_reader_status.ok()) {
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
  std::shared_ptr<::arrow::Schema> arrow_schema;
  auto included_cols = std::vector<int>{};
  auto included_rows = std::vector<int>{};
  auto schema_status = out_buffer->GetSchema(&arrow_schema);
  if (!schema_status.ok()) {
    diagnostic::error("{}", schema_status.ToStringWithoutContextLines())
      .note("failed read record batch")
      .emit(ctrl.diagnostics());
    co_return;
  }
  if (!selection.inner.fields_of_interest.empty()) {
    auto schema = type::from_arrow(*arrow_schema);
    const auto& layout = caf::get<record_type>(schema);
    for (const auto& field : selection.inner.fields_of_interest) {
      for (const auto& index : schema.resolve(field)) {
        auto field_type = layout.field(index).type;
        const auto* field_record_type = caf::get_if<record_type>(&field_type);
        if (not field_record_type) {
          included_cols.push_back(static_cast<int>(layout.flat_index(index)));
          continue;
        }
        for (const auto& leaf : field_record_type->leaves()) {
          auto nested_index = index;
          for (auto i : leaf.index) {
            nested_index.push_back(i);
          }
          included_cols.push_back(
            static_cast<int>(layout.flat_index(nested_index)));
        }
      }
    }
    std::sort(included_cols.begin(), included_cols.end());
    included_cols.erase(std::unique(included_cols.begin(), included_cols.end()),
                        included_cols.end());
    for (int i = 0; i < out_buffer->num_row_groups(); i++) {
      included_rows.push_back(i);
    }
  }
  std::unique_ptr<::arrow::RecordBatchReader> rb_reader{};
  auto record_batch_reader_status = arrow::Status{};
  if (!selection.inner.fields_of_interest.empty()) {
    auto record_batch_reader_status = out_buffer->GetRecordBatchReader(
      included_rows, included_cols, &rb_reader);
  } else {
    auto record_batch_reader_status
      = out_buffer->GetRecordBatchReader(&rb_reader);
  }
  if (!record_batch_reader_status.ok()) {
    diagnostic::error("{}",
                      record_batch_reader_status.ToStringWithoutContextLines())
      .note("failed create record batches from input data")
      .emit(ctrl.diagnostics());
    co_return;
  }
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch :
       *rb_reader) {
    if (!maybe_batch.ok()) {
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
  std::optional<located<int>> compression_level{};
  std::optional<located<std::string>> compression_type{};

  friend auto inspect(auto& f, parquet_options& x) -> bool {
    return f.object(x).fields(f.field("compression_level", x.compression_level),
                              f.field("compression_type", x.compression_type));
  }
};

class parquet_parser final : public plugin_parser {
public:
  parquet_parser() = default;
  parquet_parser(located<select_optimization> selection)
    : selection_{std::move(selection)} {
    selection_optimized = true;
  }

  auto name() const -> std::string override {
    return "parquet";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return parse_parquet(std::move(input), ctrl, selection_);
  }
  auto optimize(expression const& filter, event_order order,
                select_optimization const& selection)
    -> std::unique_ptr<plugin_parser> override {
    (void)filter;
    (void)order;
    if (selection.fields_of_interest.empty()) {
      std::make_unique<parquet_parser>();
    }
    return std::make_unique<parquet_parser>(
      located(selection, location::unknown));
  }
  friend auto inspect(auto& f, parquet_parser& x) -> bool {
    return f.object(x).fields(f.field("selection", x.selection_));
  }

private:
  located<select_optimization> selection_{};
};

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
        if (!result_compression_type.ok()) {
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
            .primary(options.compression_level->source)
            .primary(options.compression_type->source)
            .emit(ctrl.diagnostics());
        }
      } else {
        if (options.compression_level) {
          diagnostic::warning("ignoring compression level option")
            .note("has no effect without `--compression-type`")
            .primary(options.compression_level->source)
            .emit(ctrl.diagnostics());
        }
      }

      parquet_writer_props_builder.version(
        ::parquet::ParquetVersion::PARQUET_2_6);
      auto parquet_writer_props = parquet_writer_props_builder.build();
      const auto schema = input_schema.to_arrow_schema();
      auto out_buffer = std::make_shared<chunked_buffer_output_stream>();
      auto file_result = ::parquet::arrow::FileWriter::Open(
        *schema, arrow::default_memory_pool(), out_buffer,
        std::move(parquet_writer_props), std::move(arrow_writer_props));
      if (!file_result.ok()) {
        return diagnostic::error(
                 "failed to read schema",
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
      auto record_batch = to_record_batch(input);
      auto record_batch_status = writer_->WriteRecordBatch(*record_batch);
      if (!record_batch_status.ok()) {
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
      if (!close_status.ok()) {
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

class plugin final : public virtual parser_plugin<parquet_parser>,
                     public virtual printer_plugin<parquet_printer> {
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{"parquet", "https://docs.tenzir.com/"
                                             "formats/parquet"};
    parser.parse(p);
    return std::make_unique<parquet_parser>();
  }

  auto parse_printer(parser_interface& p) const
    -> std::unique_ptr<plugin_printer> override {
    auto parser = argument_parser{"parquet", "https://docs.tenzir.com/"
                                             "formats/parquet"};
    auto options = parquet_options{};
    parser.add("--compression-level", options.compression_level, "<level>");
    parser.add("--compression-type", options.compression_type, "<type>");
    parser.parse(p);
    return std::make_unique<parquet_printer>(std::move(options));
  }

  [[nodiscard]] std::string name() const override {
    return "parquet";
  }
};
} // namespace

} // namespace tenzir::plugins::parquet

// Finally, register our plugin.
TENZIR_REGISTER_PLUGIN(tenzir::plugins::parquet::plugin)
