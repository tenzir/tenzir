//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/block_builder.hpp"

#include "tenzir/nova/bitmap_iteration.hpp"

#include <algorithm>
#include <array>
#include <unordered_map>

namespace tenzir::plugins::clickhouse {

using nova::storage::BitMap;
using nova::storage::Index;

namespace {

auto absent_field(Index length) -> Field {
  return Field{null_array(length), BitMap{length, false}};
}

} // namespace

auto select_path(nova::Array<nova::Record> const& records,
                 std::span<std::string const> path) -> Field {
  TENZIR_ASSERT(not path.empty());
  auto const length = records.length();
  auto field = records.field(path.front());
  if (not field) {
    return absent_field(length);
  }
  auto result = Field{std::move(field->data), std::move(field->present)};
  for (auto const& name : path.subspan(1)) {
    auto nested = result.data.get_alternative<nova::Record>();
    if (not nested) {
      return absent_field(length);
    }
    auto child = nested->data.field(name);
    if (not child) {
      return absent_field(length);
    }
    auto present = std::move(child->present) & result.present & nested->present;
    result = Field{std::move(child->data), std::move(present)};
  }
  return result;
}

namespace {

/// Removes the fields at `paths` from `records`, one field array at a time.
auto without_paths(nova::Array<nova::Record> records,
                   std::span<std::span<std::string const> const> paths)
  -> nova::Array<nova::Record> {
  auto const length = records.length();
  auto names = std::vector<std::string_view>{};
  auto nested = std::vector<
    std::pair<std::string_view, std::vector<std::span<std::string const>>>>{};
  for (auto path : paths) {
    if (path.size() == 1) {
      names.push_back(path.front());
      continue;
    }
    auto it = std::ranges::find(nested, std::string_view{path.front()},
                                &decltype(nested)::value_type::first);
    if (it == nested.end()) {
      it = nested.insert(nested.end(), {path.front(), {}});
    }
    it->second.push_back(path.subspan(1));
  }
  if (not names.empty()) {
    records = std::move(records).without_fields(names, BitMap{length, true});
  }
  auto shapes = [](nova::Array<nova::Record> const& array) {
    return (*as<nova::storage::RecordStorage>(array.storage())).shape_indices;
  };
  for (auto const& [name, rest] : nested) {
    auto field = records.field(name);
    if (not field) {
      continue;
    }
    auto children = field->data.get_alternative<nova::Record>();
    if (not children) {
      continue;
    }
    auto const before = children->data.to_primary();
    auto after = without_paths(before, rest);
    // Drop records that lost all of their fields, but keep those that had none
    // to begin with.
    auto const old_shapes = shapes(before);
    auto const new_shapes = shapes(after);
    auto emptied = BitMap::Builder{};
    for (auto row = Index{0}; row < length; ++row) {
      auto const old_shape = old_shapes.get(row);
      emptied.emplace_back(
        children->present.get(row)
        and old_shape != nova::ShapeTable::empty_shape and old_shape >= 0
        and new_shapes.get(row) == nova::ShapeTable::empty_shape);
    }
    auto data = std::move(field->data)
                  .map_alternative<nova::Record>(
                    [&](nova::MaskedArray<nova::Array<nova::Record>>) {
                      return std::move(after);
                    });
    records = std::move(records).with_field_overwrite(
      name, Field{std::move(data), std::move(field->present)});
    auto mask = emptied.finish();
    if (mask.any()) {
      auto const removed = std::array{name};
      records = std::move(records).without_fields(removed, std::move(mask));
    }
  }
  return records;
}

/// The inputs of all columns for one batch.
struct Part {
  BitMap const& mask;
  /// The rows that have the field of each column.
  std::vector<BitMap> present;
  /// The values of each column, with nulls for selected rows without the
  /// field.
  std::vector<nova::Array<nova::Data>> data;
};

/// Rows that omit the same set of columns, as one mask per batch.
struct Partition {
  std::vector<bool> omitted;
  std::vector<BitMap> masks;
};

/// Warns about input fields that no column takes.
auto report_unwritable(RootWriter const& root,
                       nova::Array<nova::Record> const& records,
                       BitMap const& mask,
                       detail::heterogeneous_string_hashset& reported,
                       diagnostic_handler& dh) -> void {
  auto const& storage = *as<nova::storage::RecordStorage>(records.storage());
  for (auto i = size_t{0}; i < storage.names_by_index.size(); ++i) {
    auto const name = storage.names_by_index[i];
    auto const known = std::ranges::any_of(root.columns, [&](auto& column) {
      return column.name == name;
    });
    if (known or reported.contains(name)
        or not(storage.arrays[i].present & mask).any()) {
      continue;
    }
    reported.emplace(name);
    if (root.generated.contains(name)) {
      diagnostic::warning("column `{}` is a generated ClickHouse column and "
                          "cannot be written",
                          name)
        .note("the provided value is ignored; ClickHouse computes the column")
        .emit(dh);
    } else {
      diagnostic::warning("column `{}` does not exist in the ClickHouse table",
                          name)
        .note("column will be dropped")
        .emit(dh);
    }
  }
}

/// Whether the server can fill in a column that an insert omits.
auto omittable(RootWriter const& root, RootColumn const& column) -> bool {
  return column.has_default or column.writer->nullable()
         or root.catch_all.has_value();
}

/// Groups the rows by the columns that they omit. Only columns with defaults
/// need this, as a `NULL` or an implicit default can also be written.
auto partition(RootWriter const& root, std::span<Part const> parts)
  -> std::vector<Partition> {
  auto const columns = root.columns.size();
  auto total = size_t{0};
  for (auto const& part : parts) {
    total += static_cast<size_t>(part.mask.true_count());
  }
  auto omitted = std::vector<bool>(columns, false);
  auto splitting = std::vector<size_t>{};
  for (auto c = size_t{0}; c < columns; ++c) {
    auto const& column = root.columns[c];
    if (not omittable(root, column)) {
      continue;
    }
    auto absent = size_t{0};
    for (auto const& part : parts) {
      absent
        += static_cast<size_t>(part.mask.and_not(part.present[c]).true_count());
    }
    if (absent == total) {
      omitted[c] = true;
    } else if (absent != 0
               and (column.has_default
                    or (root.catch_all and not column.writer->nullable()))) {
      splitting.push_back(c);
    }
  }
  auto result = std::vector<Partition>{};
  if (splitting.empty()) {
    auto& only = result.emplace_back(std::move(omitted), std::vector<BitMap>{});
    for (auto const& part : parts) {
      only.masks.push_back(part.mask);
    }
    return result;
  }
  auto index = std::unordered_map<std::vector<bool>, size_t>{};
  auto masks = std::vector<std::vector<BitMap::Mutable>>{};
  auto key = std::vector<bool>(splitting.size());
  for (auto p = size_t{0}; p < parts.size(); ++p) {
    auto const& part = parts[p];
    auto absent = std::vector<BitMap>{};
    for (auto c : splitting) {
      absent.push_back(part.mask.and_not(part.present[c]));
    }
    for (auto row : nova::storage::true_bits(part.mask)) {
      for (auto i = size_t{0}; i < splitting.size(); ++i) {
        key[i] = absent[i].get(row);
      }
      auto [it, inserted] = index.try_emplace(key, result.size());
      if (inserted) {
        auto& added = result.emplace_back(omitted, std::vector<BitMap>{});
        for (auto i = size_t{0}; i < splitting.size(); ++i) {
          added.omitted[splitting[i]] = key[i];
        }
        auto& added_masks = masks.emplace_back();
        for (auto const& other : parts) {
          added_masks.emplace_back(other.mask.length());
        }
      }
      std::ignore = masks[it->second][p].set(row, true);
    }
  }
  for (auto i = size_t{0}; i < result.size(); ++i) {
    for (auto& mask : masks[i]) {
      result[i].masks.push_back(std::move(mask).finish());
    }
  }
  return result;
}

/// Writes the rows of `partition` into one block.
auto write(RootWriter const& root, std::span<Part const> parts,
           Partition partition, std::string_view table, diagnostic_handler& dh)
  -> Option<PreparedInsert> {
  auto ctx = WriteCtx{dh};
  auto rows = partition.masks;
  // Rows without a required column cannot be written.
  for (auto c = size_t{0}; c < root.columns.size(); ++c) {
    auto const& column = root.columns[c];
    if (partition.omitted[c] or omittable(root, column)) {
      continue;
    }
    auto reported = false;
    for (auto p = size_t{0}; p < parts.size(); ++p) {
      auto missing = rows[p].and_not(parts[p].present[c]);
      if (not missing.any()) {
        continue;
      }
      if (not reported) {
        diagnostic::warning(
          "required column missing in input, event will be dropped")
          .note("column `{}` is missing", column.name)
          .emit(dh);
        reported = true;
      }
      rows[p] = std::move(rows[p]).and_not(missing);
    }
  }
  for (auto c = size_t{0}; c < root.columns.size(); ++c) {
    if (partition.omitted[c]) {
      continue;
    }
    for (auto p = size_t{0}; p < parts.size(); ++p) {
      rows[p] = root.columns[c].writer->create_mask(parts[p].data[c],
                                                    std::move(rows[p]), ctx);
    }
  }
  auto total = uint64_t{0};
  for (auto const& mask : rows) {
    total += static_cast<uint64_t>(mask.true_count());
  }
  if (total == 0) {
    return None{};
  }
  auto block = ::clickhouse::Block{};
  auto column_parts = std::vector<ColumnPart>{};
  for (auto c = size_t{0}; c < root.columns.size(); ++c) {
    if (partition.omitted[c]) {
      continue;
    }
    column_parts.clear();
    for (auto p = size_t{0}; p < parts.size(); ++p) {
      column_parts.push_back({parts[p].data[c], rows[p]});
    }
    block.AppendColumn(root.columns[c].name,
                       root.columns[c].writer->create_column(column_parts,
                                                             ctx));
  }
  if (block.GetColumnCount() == 0) {
    diagnostic::warning(
      "no input column maps to a column in ClickHouse table `{}`", table)
      .note("{} event(s) will be dropped", total)
      .note("the ClickHouse client cannot insert rows that consist only of "
            "default columns (it would emit `INSERT INTO ... () VALUES`)")
      .emit(dh);
    return None{};
  }
  return PreparedInsert{
    std::move(block),
    total,
    std::move(partition.masks),
  };
}

} // namespace

auto reshape_for_catch_all(nova::Array<nova::Record> const& records,
                           CatchAll const& catch_all)
  -> nova::Array<nova::Record> {
  auto fields = std::vector<std::pair<std::string_view, Field>>{};
  auto paths = std::vector<std::span<std::string const>>{};
  fields.reserve(catch_all.mappings.size() + 1);
  for (auto const& mapping : catch_all.mappings) {
    fields.emplace_back(mapping.column, select_path(records, mapping.path));
    paths.emplace_back(mapping.path);
  }
  auto const length = records.length();
  fields.emplace_back(
    catch_all.column,
    Field{
      nova::Array<nova::Data>{without_paths(records.to_primary(), paths)},
      BitMap{length, true},
    });
  return nova::Array<nova::Record>::from_fields(fields);
}

auto build_inserts(RootWriter const& root, std::span<nova::Events const> events,
                   std::string_view table, diagnostic_handler& dh)
  -> std::vector<PreparedInsert> {
  auto parts = std::vector<Part>{};
  parts.reserve(events.size());
  auto reported = detail::heterogeneous_string_hashset{};
  auto collided = false;
  for (auto const& batch : events) {
    auto records = batch.data.to_primary();
    if (root.catch_all) {
      // An input field with the name of the catch-all column stays an
      // ordinary field, which therefore moves into the catch-all.
      auto const& name = root.catch_all->column;
      if (auto field = records.field(name);
          not collided and field and (field->present & batch.mask).any()) {
        diagnostic::warning("field `{}` has the name of the catch-all column",
                            name)
          .note("the field is written into the catch-all column")
          .emit(dh);
        collided = true;
      }
      records = reshape_for_catch_all(records, *root.catch_all);
    } else {
      report_unwritable(root, records, batch.mask, reported, dh);
    }
    auto& part = parts.emplace_back(batch.mask, std::vector<BitMap>{},
                                    std::vector<nova::Array<nova::Data>>{});
    part.present.reserve(root.columns.size());
    part.data.reserve(root.columns.size());
    for (auto const& column : root.columns) {
      auto field = records.field(column.name);
      if (not field) {
        part.present.emplace_back(records.length(), false);
        part.data.push_back(null_array(records.length()));
        continue;
      }
      // Writers see rows without the field as nulls.
      auto absent = batch.mask.and_not(field->present);
      part.data.push_back(std::move(field->data).null_where(std::move(absent)));
      part.present.push_back(std::move(field->present));
    }
  }
  auto result = std::vector<PreparedInsert>{};
  for (auto& rows : partition(root, parts)) {
    if (auto insert = write(root, parts, std::move(rows), table, dh)) {
      result.push_back(std::move(*insert));
    }
  }
  return result;
}

} // namespace tenzir::plugins::clickhouse
