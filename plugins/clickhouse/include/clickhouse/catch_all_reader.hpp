//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/table_description.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::clickhouse {

/// A column of a catch-all table and the event path that it fills.
struct PlacedColumn {
  std::string name;
  std::vector<ast::field_path::segment> path;
  /// Whether the column has the type `JSON`, which arrives as a string.
  bool json = false;
};

/// How `to_clickhouse` splits events across the columns of a catch-all table.
struct CatchAllLayout {
  /// The name of the catch-all column.
  std::string column;
  /// Every other column, in table order.
  std::vector<PlacedColumn> columns;

  /// Returns the column `name`, or `nullptr` if the table has no such column.
  auto find(std::string_view name) const -> PlacedColumn const*;

  /// Whether the column `name` holds the same value as the field `name` of
  /// the reconstructed events, which a predicate on the field may then use.
  auto addressable(std::string_view name) const -> bool;
};

/// Returns the layout of the table with `description`, or `None` if the table
/// has no catch-all column.
auto make_catch_all_layout(std::span<ColumnDescription const> description,
                           diagnostic_handler& dh)
  -> failure_or<Option<CatchAllLayout>>;

/// Rebuilds the events that `to_clickhouse` wrote into a catch-all table from
/// `events`, which hold the `columns` of one block in block order. The fields
/// of the catch-all column come first, followed by every other column at its
/// path. A column always wins over a catch-all field at the same path, even a
/// column that `layout` does not know. Strings of `JSON` columns become values,
/// inferring types as `parse_json` does. The rows must have been read with
/// `json_type_escape_dots_in_keys=1`, so that literal dotted keys stay apart
/// from nested paths.
auto reconstruct_catch_all(nova::Events events,
                           std::span<std::string_view const> columns,
                           CatchAllLayout const& layout, diagnostic_handler& dh)
  -> nova::Events;

} // namespace tenzir::plugins::clickhouse
