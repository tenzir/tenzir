//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/catch_all_reader.hpp"

#include "clickhouse/arguments.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/nova/event_builder.hpp"
#include "tenzir/nova/json_parser.hpp"

#include <algorithm>
#include <simdjson.h>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

auto is_json_type(std::string_view type) -> bool {
  auto const normalized = remove_non_significant_whitespace(type);
  auto inner = std::string_view{normalized};
  if (auto nullable = unwrap_clickhouse_type_call(inner, "Nullable")) {
    inner = *nullable;
  }
  return inner == "JSON" or unwrap_clickhouse_type_call(inner, "JSON");
}

/// Returns the event path of the column `name`. A name with an empty
/// component cannot come from a mapping, so it stays a single field.
auto path_of(std::string_view name) -> std::vector<ast::field_path::segment> {
  auto path = std::vector<ast::field_path::segment>{};
  for (auto component : detail::split(name, ".")) {
    if (component.empty()) {
      path.clear();
      break;
    }
    path.push_back({ast::identifier{component, location::unknown}, false});
  }
  if (path.empty()) {
    path.push_back({ast::identifier{name, location::unknown}, false});
  }
  return path;
}

/// Parses the JSON strings that ClickHouse sends for a `JSON` column. Keys
/// stay literal, so that `{"a.b": 1}` keeps its dotted field name.
auto parse_json_column(nova::Array<nova::Data> const& data,
                       std::string_view column, diagnostic_handler& dh)
  -> nova::Array<nova::Data> {
  auto const length = data.length();
  auto const strings = data.get_alternative<nova::String>();
  auto builder = nova::EventBuilder::make_prevalidated({}, dh);
  auto parser = simdjson::ondemand::parser{};
  auto buffer = std::string{};
  auto const options = nova::JsonDecodingOptions{.exact_keys = true};
  // A failure becomes one warning for the whole column below.
  auto ignored = null_diagnostic_handler{};
  auto failed = false;
  for (auto row = nova::storage::Index{0}; row < length; ++row) {
    if (not strings or not strings->present.get(row)) {
      builder.value().null();
      continue;
    }
    buffer.assign(std::string_view{*strings->data.get(row)});
    auto doc = parser.iterate(buffer);
    if (doc.error()) {
      failed = true;
      builder.value().null();
      continue;
    }
    auto& document = doc.value_unsafe();
    if (not nova::parse_json_value(document, builder.value(), ignored,
                                   std::string_view{buffer}, options)) {
      failed = true;
      builder.discard_last();
      builder.value().null();
    }
  }
  if (failed) {
    diagnostic::warning("failed to parse JSON of ClickHouse column `{}`",
                        column)
      .note("affected values are `null`")
      .emit(dh);
  }
  return builder.finish_data();
}

} // namespace

auto CatchAllLayout::find(std::string_view name) const -> PlacedColumn const* {
  auto const it = std::ranges::find(columns, name, &PlacedColumn::name);
  return it == columns.end() ? nullptr : &*it;
}

auto CatchAllLayout::addressable(std::string_view name) const -> bool {
  auto const* column = find(name);
  return column and not column->json and column->path.size() == 1
         and column->path[0].id.name == name;
}

auto make_catch_all_layout(std::span<ColumnDescription const> description,
                           diagnostic_handler& dh)
  -> failure_or<Option<CatchAllLayout>> {
  TRY(auto catch_all, find_catch_all(description, dh));
  if (not catch_all) {
    return Option<CatchAllLayout>{};
  }
  auto layout = CatchAllLayout{.column = std::move(*catch_all), .columns = {}};
  for (auto const& column : description) {
    if (column.name == layout.column) {
      continue;
    }
    layout.columns.push_back({
      .name = column.name,
      .path = path_of(column.name),
      .json = is_json_type(column.type),
    });
  }
  return Option<CatchAllLayout>{std::move(layout)};
}

auto reconstruct_catch_all(nova::Events events,
                           std::span<std::string_view const> columns,
                           CatchAllLayout const& layout, diagnostic_handler& dh)
  -> nova::Events {
  auto const length = events.length();
  auto result = nova::Array<nova::Record>::make_empty(length);
  if (auto catch_all = events.data.field(layout.column)) {
    auto parsed = nova::MaskedArray<nova::Array<nova::Data>>{
      parse_json_column(catch_all->data, layout.column, dh),
      std::move(catch_all->present),
    };
    result = nova::records_or_empty(std::move(parsed), length,
                                    location::unknown, dh);
  }
  // Every column of the block lands on top of the catch-all, so that no field
  // from the catch-all replaces a value that ClickHouse returned as a column.
  // This includes generated columns that the session's settings add to
  // `SELECT *` and columns that the table gained after it was described.
  for (auto name : columns) {
    if (name == layout.column) {
      continue;
    }
    auto field = events.data.field(name);
    if (not field) {
      continue;
    }
    auto const* known = layout.find(name);
    if (known and known->json) {
      field->data = parse_json_column(field->data, name, dh);
    }
    auto const path = known ? known->path : path_of(name);
    result = nova::assign_nested_field(std::move(result), path,
                                       std::move(*field), dh);
  }
  return nova::Events{std::move(result), std::move(events.mask),
                      std::move(events.meta)};
}

} // namespace tenzir::plugins::clickhouse
