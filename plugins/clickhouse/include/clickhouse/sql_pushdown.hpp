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
#include "tenzir/pushdown/column_model.hpp"
#include "tenzir/pushdown/renderer.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Pushdown of optimizer hints into the `SELECT` of `from_clickhouse`.
///
/// The optimizer hands `from_clickhouse` the entire filter chain that follows
/// it, plus an optional limit and projection. This module rewrites the
/// `SELECT` that the operator sends so that ClickHouse does as much of that
/// work as possible. The translation of filters is shared with other sources
/// (see `tenzir/pushdown/translate.hpp`). This module describes a ClickHouse
/// table to it, spells the result in ClickHouse SQL, and assembles the query.
///
/// TODO: Pushdown into a user-provided `sql` query, which requires parsing
/// SQL.
namespace tenzir::plugins::clickhouse {

/// The columns of a ClickHouse table.
class SqlSchema {
public:
  /// Registers a `DESCRIBE TABLE` row. Named tuple elements become nested
  /// paths, so `t Tuple(a Int64)` makes `t.a` addressable. `Nullable` and
  /// `LowCardinality` wrappers are transparent.
  ///
  /// A column whose type `from_clickhouse` cannot decode is absent from the
  /// local result, and so is a tuple with any such element. Such a column has
  /// no addressable paths: a predicate on one of its elements would match rows
  /// in SQL that the local evaluation never sees, and a narrowed projection
  /// would make an element decodable that the whole column is not.
  ///
  /// A `generated` column (`ALIAS`, `MATERIALIZED`, or `EPHEMERAL`) is not part
  /// of `SELECT *` unless session settings include it, so the local evaluation
  /// may or may not see it. Predicates on it stay local, and a projection that
  /// names it selects every column, which reproduces `SELECT *` either way.
  auto add_column(std::string_view name, std::string_view type,
                  bool generated = false) -> void;

  /// The top-level columns in table order, including generated ones.
  auto columns() const -> std::span<const std::string>;

  /// Returns whether the top-level column `name` is generated.
  auto is_generated(std::string_view name) const -> bool;

  /// The columns that predicates may reference, as TQL sees them. A column
  /// whose type has no unambiguous TQL counterpart is absent.
  auto model() const -> pushdown::ColumnModel const&;

  /// Renders the `SELECT` expression for the top-level column `name` that
  /// yields only the nested `paths` below it, or `None` if the column should
  /// be selected whole. Each path is relative to the column. The expression
  /// rebuilds the tuple structure with `CAST(tuple(...))`, so that the result
  /// decodes to the same nested record with the other elements left out.
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

  /// Builds the node for a column of `type` at `path`, and adds every
  /// comparable leaf below it to the model.
  auto make_node(std::string_view type, std::vector<std::string>& path) -> Node;

  auto find_node(std::span<const std::string> path) const -> Node const*;

  std::vector<std::string> columns_;
  detail::flat_set<std::string> generated_;
  detail::stable_map<std::string, Node> nodes_;
  pushdown::ColumnModel model_;
};

/// Spells the pushdown IR in ClickHouse SQL.
///
/// ClickHouse computes integer arithmetic in a type that holds the exact
/// result and floating-point arithmetic in `Float64`, so the default spelling
/// of arithmetic delivers the result types of the IR.
class ClickHouseRenderer final : public pushdown::SqlRenderer {
private:
  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const -> std::string override;

  auto render_double(double x) const -> std::string override;

  auto render_enum(pushdown::EnumLabel const& x) const
    -> Option<Fragment> override;

  auto render_time(pushdown::TimeValue const& x) const
    -> Option<Fragment> override;

  auto render_ip(pushdown::IpValue const& x) const -> Option<Fragment> override;

  auto render_call(pushdown::Operation op, std::span<Fragment const> args) const
    -> Option<Fragment> override;

  auto render_conditional(Fragment const& condition, Fragment const& then,
                          Fragment const& otherwise) const
    -> Option<Fragment> override;
};

/// Renders the `SELECT` for `table`. `schema` is required when `projection`
/// is set; without a schema, the projection is ignored. Each path in the
/// projection selects its top-level column, narrowed to the requested tuple
/// elements when the path is nested; if no column survives, or a path names a
/// generated column, every column is selected.
auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string;

/// Quotes `name` as a ClickHouse identifier using backticks.
auto quote_sql_identifier(std::string_view name) -> std::string;

/// Quotes `text` as a ClickHouse string literal using single quotes.
auto quote_sql_string(std::string_view text) -> std::string;

} // namespace tenzir::plugins::clickhouse
