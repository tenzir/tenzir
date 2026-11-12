//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/flat_set.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/column_model.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/pushdown/sql.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The projection of `from_clickhouse` and `from_hydrolix` (see
/// `select_columns`), which take over the stages after them (see
/// `clickhouse/query_plan.hpp`), and the query of the legacy reader that runs
/// without Nova, which receives only a projection.
///
/// The legacy reader once also applied a filter chain, in SQL where exact and
/// locally otherwise. The translation of filters is shared with other sources
/// (see `tenzir/pushdown/translate.hpp`); this module describes a ClickHouse
/// table to it, spells the result in ClickHouse SQL, and assembles the query.
/// It goes away with the legacy reader.
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

  /// Makes the scalar column `name` addressable at the nested `path`, where
  /// the reconstruction of a catch-all table places it. Tuples and types that
  /// have no TQL counterpart stay unaddressable.
  auto add_placed_column(std::string_view name, std::string_view type,
                         std::vector<std::string> path) -> void;

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
  /// decodes to the same nested record with the other elements left out. It
  /// reads the elements with `tupleElement`, which, unlike `t`.`a`, cannot
  /// resolve to a column whose name contains a dot.
  auto render_projection(std::string_view name,
                         std::span<const std::vector<std::string>> paths) const
    -> Option<std::string>;

  /// Returns the column that `add_placed_column` placed at `path`.
  auto placed_column(std::span<const std::string> path) const
    -> Option<std::string const&>;

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
  detail::flat_map<std::vector<std::string>, std::string> placed_;
  pushdown::ColumnModel model_;
};

/// Spells the pushdown IR in ClickHouse SQL, for the legacy reader, which
/// applies the predicates that it cannot spell exactly in Tenzir.
///
/// ClickHouse computes integer arithmetic in a type that holds the exact
/// result and floating-point arithmetic in `Float64`, so the default spelling
/// of arithmetic delivers the result types of the IR. With a `schema`, columns
/// placed at nested paths render as their columns.
class ClickHouseRenderer final : public pushdown::SqlRenderer {
public:
  explicit ClickHouseRenderer(SqlSchema const* schema = nullptr)
    : schema_{schema} {
  }

private:
  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const -> std::string override;

  auto render_double(double x) const -> std::string override;

  auto render_enum(pushdown::EnumLabel const& x) const
    -> Option<Fragment> override;

  auto render_time(pushdown::TimeValue const& x) const
    -> Option<Fragment> override;

  auto render_ip(pushdown::IpValue const& x) const -> Option<Fragment> override;

  auto render_column(std::span<std::string const> path) const
    -> Option<Fragment> override;

  auto
  render_call(pushdown::Call const& x, std::span<Fragment const> args) const
    -> Option<Fragment> override;

  auto render_conditional(Fragment const& condition, Fragment const& then,
                          Fragment const& otherwise) const
    -> Option<Fragment> override;

  SqlSchema const* schema_;
};

/// Selects the top-level columns of `schema` that `projection` names, narrowed
/// to the requested tuple elements, or `*`. Without a schema or a projection,
/// if no column survives, or if a path names a generated column, the
/// selection is `*`, which reproduces `SELECT *` under any session settings.
auto select_columns(SqlSchema const* schema,
                    Option<ir::OptimizeProjection> const& projection)
  -> std::string;

/// Renders the `SELECT` for `table`. `schema` is required when `projection`
/// is set; without a schema, the projection is ignored. Each path in the
/// projection selects its top-level column, narrowed to the requested tuple
/// elements when the path is nested; if no column survives, or a path names a
/// generated column, every column is selected.
auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string;

/// Plans a read of a table with `schema`: pushes the predicates of `filter`
/// that ClickHouse evaluates exactly like TQL, and selects the columns that
/// `projection` names, as `make_select_query` does.
auto make_table_plan(SqlSchema const& schema, ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection,
                     registry const& reg) -> pushdown::TablePlan;

/// Renders the `SELECT` that reads `table` according to `plan`, with `LIMIT
/// limit` if every predicate was pushed.
auto make_table_query(pushdown::TablePlan const& plan, std::string_view table,
                      Option<uint64_t> limit) -> std::string;

/// Quotes `name` as a ClickHouse identifier using backticks.
auto quote_sql_identifier(std::string_view name) -> std::string;

/// Quotes `text` as a ClickHouse string literal using single quotes.
auto quote_sql_string(std::string_view text) -> std::string;

} // namespace tenzir::plugins::clickhouse
