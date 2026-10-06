//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/column_writer.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/column_writer_detail.hpp"
#include "tenzir/detail/string.hpp"

#include <clickhouse/columns/array.h>
#include <clickhouse/columns/nullable.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/tuple.h>

#include <algorithm>
#include <string_view>

namespace tenzir::plugins::clickhouse {

using nova::storage::BitMap;
using nova::storage::Index;

auto WriteCtx::first_report(ColumnWriter const& writer, WriteIssue issue)
  -> bool {
  auto const bit = uint32_t{1} << static_cast<uint8_t>(issue);
  auto it = std::ranges::find(reported_, &writer,
                              &decltype(reported_)::value_type::first);
  if (it == reported_.end()) {
    reported_.emplace_back(&writer, bit);
    return true;
  }
  auto const first = (it->second & bit) == 0;
  it->second |= bit;
  return first;
}

auto WriteCtx::first_unknown_report(ColumnWriter const& writer,
                                    std::string_view name) -> bool {
  auto const reported
    = std::ranges::any_of(reported_unknown_, [&](auto const& entry) {
        return entry.first == &writer and entry.second == name;
      });
  if (reported) {
    return false;
  }
  reported_unknown_.emplace_back(&writer, std::string{name});
  return true;
}

auto ColumnWriter::stores_null() const -> bool {
  return unwrap_clickhouse_type_call(clickhouse_type_, "Nullable").has_value();
}

auto null_rows(nova::Array<nova::Data> const& data) -> BitMap {
  auto nulls = data.get_alternative<nova::Null>();
  return nulls ? std::move(nulls->present) : BitMap{data.length(), false};
}

auto first_row(BitMap const& rows) -> Index {
  auto bits = nova::storage::true_bits(rows);
  auto it = bits.begin();
  TENZIR_ASSERT(it != bits.end());
  return *it;
}

auto total_rows(std::span<ColumnPart const> parts) -> size_t {
  auto result = size_t{0};
  for (auto const& part : parts) {
    result += static_cast<size_t>(part.present.true_count());
  }
  return result;
}

auto wrap_nullable(::clickhouse::ColumnRef nested,
                   Option<std::vector<uint8_t>> nulls)
  -> ::clickhouse::ColumnRef {
  if (not nulls) {
    return nested;
  }
  return std::make_shared<::clickhouse::ColumnNullable>(
    std::move(nested),
    std::make_shared<::clickhouse::ColumnUInt8>(std::move(*nulls)));
}

auto report_null(ColumnWriter const& writer, WriteCtx& ctx) -> void {
  if (ctx.first_report(writer, WriteIssue::null)) {
    diagnostic::warning("column `{}` contains `null`, but the ClickHouse "
                        "column is not nullable",
                        writer.path())
      .note("affected events will be dropped")
      .emit(ctx.dh());
  }
}

auto report_mismatch(ColumnWriter const& writer, std::string_view expected,
                     nova::Array<nova::Data> const& data, BitMap const& rows,
                     WriteCtx& ctx) -> void {
  if (ctx.first_report(writer, WriteIssue::mismatch)) {
    diagnostic::warning("incompatible type for column `{}`", writer.path())
      .note("expected `{}`, got `{}`", expected, kind_at(data, first_row(rows)))
      .emit(ctx.dh());
  }
}

auto report_unsupported(ColumnWriter const& writer, std::string_view note,
                        WriteCtx& ctx) -> void {
  if (ctx.first_report(writer, WriteIssue::unsupported)) {
    diagnostic::warning("column `{}` has an unsupported ClickHouse type `{}` "
                        "and cannot be written",
                        writer.path(), writer.clickhouse_type())
      .note("{}", note)
      .note("event will be dropped")
      .emit(ctx.dh());
  }
}

auto kind_at(nova::Array<nova::Data> const& array, Index row)
  -> std::string_view {
  auto name = []<class Tag>(nova::Array<Tag> const&) -> std::string_view {
    return nova::Type<Tag>::static_name;
  };
  return match(
    array, name, [&](nova::UnionArray const& alternatives) -> std::string_view {
      auto const& field
        = alternatives.fields()[alternatives.alternative_index_at(row)];
      return match(field.data, name);
    });
}

auto null_array(Index length) -> nova::Array<nova::Data> {
  return nova::Array<nova::Data>{
    nova::Array<nova::Null>{nova::storage::NullStorage{length}}};
}

namespace {

auto join_path(std::string_view parent, std::string_view name) -> std::string {
  return fmt::format("{}.{}", parent, name);
}

/// The lists of one batch. A constant list is expanded once, to a single row.
class Lists {
public:
  explicit Lists(nova::Array<nova::Data> const& data)
    : lists_{data.get_alternative<nova::List>()} {
    if (not lists_) {
      return;
    }
    using Constant
      = nova::storage::ConstantStorage<nova::List, nova::RowView<nova::List>>;
    auto const* constant = try_as<Constant>(lists_->data.storage());
    constant_ = constant != nullptr;
    auto primary
      = constant
          ? nova::Array<nova::List>{Constant{1, constant->value()}}.to_primary()
          : lists_->data.to_primary();
    storage_ = as<nova::storage::ListStorage>(primary.storage());
  }

  /// The rows that are lists.
  auto rows(Index length) const -> BitMap {
    return lists_ ? lists_->present : BitMap{length, false};
  }

  auto is_list(Index row) const -> bool {
    return lists_ and lists_->present.get(row);
  }

  auto span(Index row) const -> nova::storage::Span {
    return storage_->spans()[constant_ ? 0 : row];
  }

  /// The elements of all lists. Must only be called if there are lists.
  auto values() const -> nova::Array<nova::Data> const& {
    return storage_->values();
  }

private:
  Option<nova::MaskedArray<nova::Array<nova::List>>> lists_;
  bool constant_ = false;
  Option<nova::storage::ListStorage> storage_;
};

/// Removes the list rows from `rows` that have an element that `elements`
/// cannot write.
auto check_elements(Lists const& lists, BitMap rows,
                    ColumnWriter const& elements, WriteCtx& ctx) -> BitMap {
  auto const candidates = rows & lists.rows(rows.length());
  if (not candidates.any()) {
    return rows;
  }
  auto const values_length = lists.values().length();
  auto selected = BitMap::Mutable{values_length};
  for (auto row : nova::storage::true_bits(candidates)) {
    auto const span = lists.span(row);
    for (auto i = span.begin; i < span.end; ++i) {
      std::ignore = selected.set(i, true);
    }
  }
  auto const all = std::move(selected).finish();
  auto const kept = elements.create_mask(lists.values(), all, ctx);
  auto const failed = all.and_not(kept);
  if (not failed.any()) {
    return rows;
  }
  auto rejected = BitMap::Mutable{rows.length()};
  for (auto row : nova::storage::true_bits(candidates)) {
    auto const span = lists.span(row);
    for (auto i = span.begin; i < span.end; ++i) {
      if (failed.get(i)) {
        std::ignore = rejected.set(row, true);
        break;
      }
    }
  }
  return std::move(rows).and_not(std::move(rejected).finish());
}

/// Writes `Array(T)`.
class ArrayWriter final : public ColumnWriter {
public:
  ArrayWriter(std::string path, std::string type, Box<ColumnWriter> elements,
              ColumnMapping mapping)
    : ColumnWriter{std::move(path), std::move(type), elements->nullable()},
      elements_{std::move(elements)},
      mapping_{mapping} {
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    auto const lists = Lists{data};
    auto const nulls = null_rows(data);
    auto const list_rows = lists.rows(data.length());
    // Nulls keep their legacy meaning of an empty list for plain columns.
    auto const empty_nulls = mapping_ == ColumnMapping::legacy;
    if (not empty_nulls and (rows & nulls).any()) {
      report_null(*this, ctx);
    }
    if (auto mismatch = rows.and_not(list_rows | nulls); mismatch.any()) {
      report_mismatch(*this, "list", data, mismatch, ctx);
    }
    rows = std::move(rows) & (empty_nulls ? list_rows | nulls : list_rows);
    return check_elements(lists, std::move(rows), *elements_, ctx);
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    auto ends = std::vector<uint64_t>{};
    ends.reserve(total_rows(parts));
    auto elements = std::vector<ColumnPart>{};
    auto end = uint64_t{0};
    for (auto const& part : parts) {
      auto const lists = Lists{part.data};
      // Kept rows whose spans ascend without overlap share one part of
      // elements. Other spans start a new part over the same values.
      auto run = Option<BitMap::Mutable>{};
      auto run_end = Index{0};
      auto close_run = [&] {
        if (run) {
          elements.push_back({lists.values(), std::move(*run).finish()});
          run = None{};
        }
      };
      for (auto row : nova::storage::true_bits(part.present)) {
        if (lists.is_list(row)) {
          auto const span = lists.span(row);
          if (span.end > span.begin) {
            if (run and span.begin < run_end) {
              close_run();
            }
            if (not run) {
              run.emplace(lists.values().length());
            }
            for (auto i = span.begin; i < span.end; ++i) {
              std::ignore = run->set(i, true);
            }
            run_end = span.end;
            end += static_cast<uint64_t>(span.end - span.begin);
          }
        }
        ends.push_back(end);
      }
      close_run();
    }
    auto values = elements_->create_column(elements, ctx);
    return std::make_shared<::clickhouse::ColumnArray>(
      std::move(values),
      std::make_shared<::clickhouse::ColumnUInt64>(std::move(ends)));
  }

private:
  Box<ColumnWriter> elements_;
  ColumnMapping mapping_;
};

/// Writes blobs to `Array(UInt8)`. Mapped columns also accept lists of
/// integers.
class BlobWriter final : public ColumnWriter {
public:
  BlobWriter(std::string path, ColumnMapping mapping)
    : ColumnWriter{path, "Array(UInt8)", true},
      bytes_{make_bytes_writer(path + ".[]")},
      mapping_{mapping} {
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    auto const length = data.length();
    auto const lossless = mapping_ == ColumnMapping::lossless;
    auto const blobs = data.get_alternative<nova::Blob>();
    auto const lists = Lists{data};
    auto const nulls = null_rows(data);
    auto const blob_rows = blobs ? blobs->present : BitMap{length, false};
    auto const list_rows
      = lossless ? lists.rows(length) : BitMap{length, false};
    if (lossless and (rows & nulls).any()) {
      report_null(*this, ctx);
    }
    if (auto mismatch = rows.and_not(blob_rows | list_rows | nulls);
        mismatch.any()) {
      report_mismatch(*this, "blob", data, mismatch, ctx);
    }
    auto accepted = blob_rows | list_rows;
    if (not lossless) {
      accepted = std::move(accepted) | nulls;
    }
    rows = std::move(rows) & accepted;
    if (not lossless) {
      return rows;
    }
    return check_elements(lists, std::move(rows), *bytes_, ctx);
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    TENZIR_UNUSED(ctx);
    auto bytes = std::vector<uint8_t>{};
    auto ends = std::vector<uint64_t>{};
    ends.reserve(total_rows(parts));
    for (auto const& part : parts) {
      auto const blobs = part.data.get_alternative<nova::Blob>();
      auto const lists = Lists{part.data};
      for (auto row : nova::storage::true_bits(part.present)) {
        if (blobs and blobs->present.get(row)) {
          for (auto byte : *blobs->data.get(row)) {
            bytes.push_back(static_cast<uint8_t>(byte));
          }
        } else if (lists.is_list(row)) {
          auto const span = lists.span(row);
          for (auto i = span.begin; i < span.end; ++i) {
            auto const value = lists.values().get(i);
            auto const* x = try_as<nova::RowView<nova::UInt>>(value);
            TENZIR_ASSERT(x);
            bytes.push_back(static_cast<uint8_t>(**x));
          }
        }
        ends.push_back(bytes.size());
      }
    }
    return std::make_shared<::clickhouse::ColumnArray>(
      std::make_shared<::clickhouse::ColumnUInt8>(std::move(bytes)),
      std::make_shared<::clickhouse::ColumnUInt64>(std::move(ends)));
  }

private:
  static auto make_bytes_writer(std::string path) -> Box<ColumnWriter> {
    auto writer = make_scalar_writer(std::move(path), "UInt8", false,
                                     ColumnMapping::lossless);
    TENZIR_ASSERT(writer);
    return std::move(*writer);
  }

  Box<ColumnWriter> bytes_;
  ColumnMapping mapping_;
};

/// Writes `Tuple(...)` from records.
class TupleWriter final : public ColumnWriter {
public:
  struct Field {
    std::string name;
    Box<ColumnWriter> writer;
  };

  TupleWriter(std::string path, std::string type, std::vector<Field> fields)
    : ColumnWriter{std::move(path), std::move(type),
                   std::ranges::all_of(fields,
                                       [](Field const& field) {
                                         return field.writer->nullable();
                                       })},
      fields_{std::move(fields)} {
    TENZIR_ASSERT(not fields_.empty());
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    auto const records = Records{*this, data};
    auto const nulls = null_rows(data);
    if (not nullable() and (rows & nulls).any()) {
      report_null(*this, ctx);
    }
    if (auto mismatch = rows.and_not(records.rows | nulls); mismatch.any()) {
      report_mismatch(*this, "record", data, mismatch, ctx);
    }
    rows = std::move(rows) & (nullable() ? records.rows | nulls : records.rows);
    records.report_unknown(rows, ctx);
    for (auto i = size_t{0}; i < fields_.size(); ++i) {
      rows = fields_[i].writer->create_mask(records.fields[i], std::move(rows),
                                            ctx);
    }
    return rows;
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    auto records = std::vector<Records>{};
    records.reserve(parts.size());
    for (auto const& part : parts) {
      records.emplace_back(*this, part.data);
    }
    auto columns = std::vector<::clickhouse::ColumnRef>{};
    columns.reserve(fields_.size());
    auto field_parts = std::vector<ColumnPart>{};
    for (auto i = size_t{0}; i < fields_.size(); ++i) {
      field_parts.clear();
      for (auto p = size_t{0}; p < parts.size(); ++p) {
        field_parts.push_back({records[p].fields[i], parts[p].present});
      }
      columns.push_back(fields_[i].writer->create_column(field_parts, ctx));
    }
    return std::make_shared<::clickhouse::ColumnTuple>(columns);
  }

private:
  /// The records of one batch, split into the arrays of the fields. Rows that
  /// lack a field, or that are no record, are nulls in the field array.
  struct Records {
    Records(TupleWriter const& writer, nova::Array<nova::Data> const& data)
      : writer{writer} {
      auto const length = data.length();
      auto alternative = data.get_alternative<nova::Record>();
      rows = alternative ? alternative->present : BitMap{length, false};
      fields.reserve(writer.fields_.size());
      if (not alternative) {
        for (auto i = size_t{0}; i < writer.fields_.size(); ++i) {
          fields.push_back(null_array(length));
        }
        return;
      }
      primary = alternative->data.to_primary();
      auto const& storage
        = *as<nova::storage::RecordStorage>(primary->storage());
      for (auto const& field : writer.fields_) {
        auto it = storage.names.find(field.name);
        if (it == storage.names.end()) {
          fields.push_back(null_array(length));
          continue;
        }
        auto const& array = storage.arrays[it->second];
        auto const present = array.present & rows;
        fields.push_back(array.data.null_where(present.make_inverted()));
      }
    }

    /// Warns about fields that the tuple does not have.
    auto report_unknown(BitMap const& selected, WriteCtx& ctx) const -> void {
      if (not primary) {
        return;
      }
      auto const& storage
        = *as<nova::storage::RecordStorage>(primary->storage());
      for (auto i = size_t{0}; i < storage.names_by_index.size(); ++i) {
        auto const name = storage.names_by_index[i];
        auto const known
          = std::ranges::any_of(writer.fields_, [&](Field const& field) {
              return field.name == name;
            });
        if (known or not(storage.arrays[i].present & selected).any()) {
          continue;
        }
        if (ctx.first_unknown_report(writer, name)) {
          diagnostic::warning("`{}` does not exist in ClickHouse table",
                              join_path(writer.path(), name))
            .note("column will be dropped")
            .emit(ctx.dh());
        }
      }
    }

    TupleWriter const& writer;
    /// The rows that are records.
    BitMap rows;
    Option<nova::Array<nova::Record>> primary;
    std::vector<nova::Array<nova::Data>> fields;
  };

  std::vector<Field> fields_;
};

/// Writes a top-level column of an unsupported type that has a default. Such a
/// column must be omitted from the input, so every row that has it is
/// rejected.
class DefaultOnlyWriter final : public ColumnWriter {
public:
  DefaultOnlyWriter(std::string path, std::string type)
    : ColumnWriter{std::move(path), std::move(type), true} {
  }

  auto create_mask(nova::Array<nova::Data> const& data, BitMap rows,
                   WriteCtx& ctx) const -> BitMap override {
    if (rows.any()) {
      report_unsupported(*this,
                         "the column has a default value and must be omitted "
                         "from the input",
                         ctx);
    }
    return BitMap{data.length(), false};
  }

  auto create_column(std::span<ColumnPart const> parts, WriteCtx& ctx) const
    -> ::clickhouse::ColumnRef override {
    TENZIR_UNUSED(parts, ctx);
    TENZIR_UNREACHABLE();
  }
};

auto emit_unsupported_type(std::string_view path, std::string_view type,
                           diagnostic_handler& dh) -> void {
  auto diag = diagnostic::error(
    "ClickHouse column `{}` has unsupported ClickHouse type `{}`", path, type);
  if (type.starts_with("Date")) {
    diag = std::move(diag).hint("use `DateTime64(9)` instead");
  } else if (type.starts_with("UInt")) {
    diag = std::move(diag).hint("use `UInt64` instead");
  } else if (type.starts_with("Int")) {
    diag = std::move(diag).hint("use `Int64` instead");
  } else if (type.starts_with("Float")) {
    diag = std::move(diag).hint("use `Float64` instead");
  } else if (type == "IPv4") {
    diag = std::move(diag).hint("use `IPv6` instead");
  }
  std::move(diag).emit(dh);
}

auto make_tuple_writer(std::string path, std::string_view type,
                       diagnostic_handler& dh) -> Option<Box<ColumnWriter>> {
  auto elements = unwrap_clickhouse_type_call(type, "Tuple");
  TENZIR_ASSERT(elements);
  if (elements->empty()) {
    diagnostic::error("ClickHouse column `{}` is an empty record, which is not "
                      "supported",
                      path)
      .emit(dh);
    return None{};
  }
  auto fields = std::vector<TupleWriter::Field>{};
  for (auto element : split_top_level_clickhouse_type_arguments(*elements)) {
    auto split = find_top_level_clickhouse_type_space(element);
    if (split == std::string_view::npos) {
      diagnostic::error("ClickHouse column `{}` has malformed tuple element "
                        "`{}`",
                        path, element)
        .emit(dh);
      return None{};
    }
    auto name
      = unquote_identifier_component(detail::trim(element.substr(0, split)));
    auto writer = make_column_writer(join_path(path, name),
                                     detail::trim(element.substr(split + 1)),
                                     ColumnMapping::legacy, dh);
    if (not writer) {
      return None{};
    }
    fields.push_back({std::move(name), std::move(*writer)});
  }
  return make_boxed<ColumnWriter, TupleWriter>(
    std::move(path), std::string{type}, std::move(fields));
}

auto make_array_writer(std::string path, std::string_view type,
                       ColumnMapping mapping, diagnostic_handler& dh)
  -> Option<Box<ColumnWriter>> {
  auto element_type = unwrap_clickhouse_type_call(type, "Array");
  TENZIR_ASSERT(element_type);
  auto elements
    = make_column_writer(join_path(path, "[]"), *element_type, mapping, dh);
  if (not elements) {
    return None{};
  }
  return make_boxed<ColumnWriter, ArrayWriter>(
    std::move(path), std::string{type}, std::move(*elements), mapping);
}

} // namespace

auto make_column_writer(std::string path, std::string_view type,
                        ColumnMapping mapping, diagnostic_handler& dh)
  -> Option<Box<ColumnWriter>> {
  auto const inner = unwrap_clickhouse_type_call(type, "Nullable");
  auto const nullable = inner.has_value();
  if (auto writer
      = make_scalar_writer(path, nullable ? *inner : type, nullable, mapping)) {
    return writer;
  }
  if (type == "Tuple(ip IPv6,length UInt8)") {
    return make_subnet_writer(std::move(path), std::string{type}, false);
  }
  if (type == "Tuple(ip Nullable(IPv6),length Nullable(UInt8))") {
    return make_subnet_writer(std::move(path), std::string{type}, true);
  }
  if (type == "Array(UInt8)") {
    return make_boxed<ColumnWriter, BlobWriter>(std::move(path), mapping);
  }
  auto const json = nullable ? *inner : type;
  if (json == "JSON" or json.starts_with("JSON(")) {
    return make_json_writer(std::move(path), std::string{type}, nullable);
  }
  if (auto lowcard = unwrap_clickhouse_type_call(type, "LowCardinality")) {
    // The server converts plain columns, but the client only supports strings.
    if (not is_lowcardinality_supported_inner(*lowcard)) {
      diagnostic::error("ClickHouse column `{}` has unsupported type `{}`",
                        path, type)
        .note("`LowCardinality` is only supported for `String` columns")
        .emit(dh);
      return None{};
    }
    return make_column_writer(std::move(path), *lowcard, mapping, dh);
  }
  if (type.starts_with("Tuple(")) {
    return make_tuple_writer(std::move(path), type, dh);
  }
  if (type.starts_with("Array(")) {
    return make_array_writer(std::move(path), type, mapping, dh);
  }
  emit_unsupported_type(path, type, dh);
  return None{};
}

auto make_default_only_writer(std::string path, std::string clickhouse_type)
  -> Box<ColumnWriter> {
  return make_boxed<ColumnWriter, DefaultOnlyWriter>(
    std::move(path), std::move(clickhouse_type));
}

} // namespace tenzir::plugins::clickhouse
