//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/detail/flat_set.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The projection of `from_clickhouse` and `from_hydrolix` (see
/// `select_columns`), which take over the stages after them (see
/// `clickhouse/query_plan.hpp`), and the quoting of ClickHouse SQL.
namespace tenzir::plugins::clickhouse {

/// The columns of a ClickHouse table.
class SqlSchema {
public:
  /// Registers a `DESCRIBE TABLE` row. Named tuple elements become nested
  /// paths, so `t Tuple(a Int64)` makes `t.a` addressable. `Nullable` and
  /// `LowCardinality` wrappers are transparent.
  ///
  /// A column whose type `from_clickhouse` cannot decode is absent from the
  /// result, and so is a tuple with any such element. Such a column has no
  /// addressable paths: a narrowed projection would make an element decodable
  /// that the whole column is not.
  ///
  /// A `generated` column (`ALIAS`, `MATERIALIZED`, or `EPHEMERAL`) is not part
  /// of `SELECT *` unless session settings include it, so a projection that
  /// names it selects every column, which reproduces `SELECT *` either way.
  auto add_column(std::string_view name, std::string_view type,
                  bool generated = false) -> void;

  /// The top-level columns in table order, including generated ones.
  auto columns() const -> std::span<const std::string>;

  /// Returns whether the top-level column `name` is generated.
  auto is_generated(std::string_view name) const -> bool;

  /// Renders the `SELECT` expression for the top-level column `name` that
  /// yields only the nested `paths` below it, or `None` if the column should
  /// be selected whole. Each path is relative to the column. The expression
  /// rebuilds the tuple structure with `CAST(tuple(...))`, so that the result
  /// decodes to the same nested record with the other elements left out. It
  /// reads the elements with `tupleElement`, which, unlike `t`.`a`, cannot
  /// resolve to a column whose name contains a dot.
  auto render_projection(std::string_view name,
                         std::span<const std::vector<std::string>> paths) const
    -> Option<std::string>;

private:
  /// A node of a column's type structure. Tuples have children; every other
  /// type is a leaf.
  struct Node {
    /// The ClickHouse type text, with insignificant whitespace removed.
    std::string type;
    /// Named tuple elements in declaration order. Boxed because the type
    /// recurses through the map's value type.
    detail::stable_map<std::string, Box<Node>> children;
  };

  /// Builds the node for a column of `type`.
  static auto make_node(std::string_view type) -> Node;

  auto find_node(std::span<const std::string> path) const -> Node const*;

  std::vector<std::string> columns_;
  detail::flat_set<std::string> generated_;
  detail::stable_map<std::string, Node> nodes_;
};

/// Selects the top-level columns of `schema` that `projection` names, narrowed
/// to the requested tuple elements, or `*`. Without a schema or a projection,
/// if no column survives, or if a path names a generated column, the
/// selection is `*`, which reproduces `SELECT *` under any session settings.
auto select_columns(SqlSchema const* schema,
                    Option<ir::OptimizeProjection> const& projection)
  -> std::string;

/// Quotes `name` as a ClickHouse identifier using backticks.
auto quote_sql_identifier(std::string_view name) -> std::string;

/// Quotes `text` as a ClickHouse string literal using single quotes.
auto quote_sql_string(std::string_view text) -> std::string;

} // namespace tenzir::plugins::clickhouse
