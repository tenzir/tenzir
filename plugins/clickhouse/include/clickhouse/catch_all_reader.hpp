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

#include <clickhouse/block.h>

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
  /// Whether `SELECT *` may leave out the column, depending on session
  /// settings.
  bool generated = false;
};

/// The column whose value reconstruction places at a path of the events.
struct FieldOrigin {
  /// The name of the column.
  std::string column;
  /// Whether the column has the type `JSON`. The catch-all column has it.
  bool json = false;
  /// The path below the value of the column, which is empty if the field is
  /// the whole value.
  std::vector<std::string> path;
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

  /// Returns the column whose value holds the field at `path` of the
  /// reconstructed events in every row, or `None` if that depends on the row
  /// or on session settings.
  ///
  /// A column takes the field at its path and everything below it, so a
  /// column whose path is a prefix of `path` holds the field. A column whose
  /// path extends `path` merges into the field, which then has no single
  /// origin. Neither has a field related to more than one column, since
  /// the order of the columns in a block decides which one wins, nor a field
  /// related to a generated column, which `SELECT *` may leave out. A field
  /// related to no column comes from the catch-all column.
  auto origin(std::span<std::string const> path) const -> Option<FieldOrigin>;
};

/// Returns the layout of the table with `description`, or `None` if the table
/// has no catch-all column.
auto make_catch_all_layout(std::span<ColumnDescription const> description,
                           diagnostic_handler& dh)
  -> failure_or<Option<CatchAllLayout>>;

/// Rejects a block with columns added after `layout` was described. Such a
/// column can override a field that a pushed predicate read from the
/// catch-all instead. Check the zero-row result header too, before decoding:
/// the stale predicate may have removed every matching row.
auto validate_catch_all_block(::clickhouse::Block const& block,
                              CatchAllLayout const& layout,
                              std::string_view service, diagnostic_handler& dh)
  -> failure_or<void>;

/// Rebuilds the events that `to_clickhouse` wrote into a catch-all table from
/// `events`, which hold the `columns` of one block in block order. The fields
/// of the catch-all column come first, followed by every other column at its
/// path. A column always wins over a catch-all field at the same path, even a
/// column that `layout` does not know. Strings of `JSON` columns become values,
/// inferring types as `parse_json` does. The rows must have been read with
/// `json_type_escape_dots_in_keys=1`, so that literal dotted keys stay apart
/// from nested paths. `service` names the backend in warnings, such as
/// `ClickHouse` or `Hydrolix`.
auto reconstruct_catch_all(nova::Events events,
                           std::span<std::string_view const> columns,
                           CatchAllLayout const& layout,
                           std::string_view service, diagnostic_handler& dh)
  -> nova::Events;

} // namespace tenzir::plugins::clickhouse
