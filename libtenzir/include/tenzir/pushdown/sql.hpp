//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/column_model.hpp"
#include "tenzir/pushdown/expr.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/pushdown/translate.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Assembly of the `SELECT` that a SQL source sends for a table.
///
/// In `table` mode, a SQL source reads a table with a `SELECT` that carries
/// the optimizer's hints: the predicates that the target evaluates exactly
/// like TQL, the columns that downstream needs, a limit, or an aggregation. A
/// source renders the parts of the query, such as predicates with a
/// `SqlRenderer`, and this module assembles them into a statement. What SQL
/// dialects disagree on, such as how to spell a limit or a count, is up to
/// the source's dialect.
namespace tenzir::pushdown {

struct AggregationPlan;

/// A `SELECT` from rendered parts.
struct SqlSelect {
  /// The selected expressions, as they appear after `SELECT`.
  std::string selection = "*";
  /// The relation to read, as it appears after `FROM`.
  std::string from;
  /// Predicates that the target evaluates, to be joined with `AND`.
  std::vector<std::string> where;
  /// The expressions to group by.
  std::vector<std::string> group_by;
  /// The number of rows to return at most.
  Option<uint64_t> limit;
};

/// Where a SQL dialect spells the limit of a `SELECT`.
enum class LimitClause {
  /// `LIMIT n` at the end, as in most dialects.
  limit,
  /// `TOP (n)` after `SELECT`, as in T-SQL.
  top,
};

/// The spelling of the parts of a `SELECT` that SQL dialects disagree on.
class SqlQueryDialect {
public:
  virtual ~SqlQueryDialect() = default;

  /// Quotes an identifier, such as a column name or the alias of a result
  /// column.
  virtual auto quote_identifier(std::string_view name) const -> std::string = 0;

  /// Where the dialect spells a limit. Defaults to `LimitClause::limit`.
  virtual auto limit_clause() const -> LimitClause;

  /// The largest limit that the target accepts. A target that fails the query
  /// on a larger one overrides this. Defaults to the largest `uint64_t`.
  virtual auto max_limit() const -> uint64_t;
};

/// Spells `select` in `dialect`.
auto render_select(SqlSelect const& select, SqlQueryDialect const& dialect)
  -> std::string;

/// A column of a table, as a `SELECT` sees it.
struct SqlColumn {
  std::string name;
  /// Whether `SELECT *` returns the column.
  bool visible = true;
};

/// How one read of a table splits the work between the target and the
/// operator.
struct TablePlan {
  /// The columns to select, as they appear after `SELECT`.
  std::string selection = "*";
  /// Predicates that the target evaluates, to be joined with `AND`.
  std::vector<std::string> pushed;
  /// Predicates that the operator evaluates, in order.
  ir::OptimizeFilter local;
};

/// Plans a read of a table with `columns`, in table order, which are empty if
/// the table is unknown. Pushes the predicates of `filter` that `renderer`
/// spells exactly for `model`, and selects the visible columns that
/// `projection`, the remaining predicates, and `keep`, such as a tracking
/// column, need.
///
/// Many targets resolve column names regardless of case, but label a result
/// column as the query spells it. Only the exact name of a column keeps the
/// field that `SELECT *` would produce, so the plan selects columns by their
/// exact names, in table order, and leaves other names to downstream. A plan
/// that needs no fields still needs the rows, and selects the first visible
/// column. Without columns, nothing translates, and the plan reads the table
/// with `SELECT *` as it is, which fails as it would without any hints.
auto make_table_plan(std::span<SqlColumn const> columns,
                     ColumnModel const& model, Renderer const& renderer,
                     SqlQueryDialect const& dialect, ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection,
                     registry const& reg,
                     std::span<std::string const> keep = {}) -> TablePlan;

/// Assembles the `SELECT` that reads `from` according to `plan`. The query
/// carries `limit` if no predicate stays local and `dialect` accepts it,
/// including a limit of zero.
auto make_table_select(TablePlan const& plan, std::string from,
                       Option<uint64_t> limit, SqlQueryDialect const& dialect)
  -> SqlSelect;

/// The extremes that `SqlAggregateDialect::render_extremum` spells.
enum class Extremum {
  min,
  max,
};

/// The spelling of an aggregation over a table in a SQL dialect, which also
/// decides which columns the target computes as TQL does.
///
/// Unlike a `SqlQueryDialect`, it depends on the table: on the names of its
/// columns, and on how the source reads them. The hooks receive each input as
/// `translate_field` describes it: the column that holds the field, and the
/// rendered expression of its value.
class SqlAggregateDialect {
public:
  virtual ~SqlAggregateDialect() = default;

  /// Returns whether a result column named `alias` could change what the
  /// query reads, as when the table has a column of that name.
  virtual auto alias_collides(std::string_view alias) const -> bool = 0;

  /// Renders the value of a field for a slot that expects a value, such as an
  /// output column or a group key, or returns `None` if the target cannot
  /// spell it.
  virtual auto render_value(Expr const& expr) const -> Option<std::string> = 0;

  /// Spells the selected value of a group key on the field at `path`, or
  /// returns `None` if the target does not group `field` as TQL does. The
  /// query groups by `value`, the rendered value of the field.
  virtual auto
  render_key(std::span<std::string const> path, FieldValue const& field,
             std::string const& value) const -> Option<std::string>
    = 0;

  /// Spells `count()` without `value`, or `count(x)`, where `value` is the
  /// rendered value of `x`. Both must yield an integer that TQL reads as
  /// `int`.
  virtual auto render_count(Option<std::string> const& value) const
    -> std::string
    = 0;

  /// Spells `min(x)` or `max(x)`, where `value` is the rendered value of the
  /// field `x`, or returns `None` if the target does not compute `extremum`
  /// of `field` as TQL does.
  virtual auto render_extremum(Extremum extremum, FieldValue const& field,
                               std::string const& value) const
    -> Option<std::string>
    = 0;
};

/// Assembles a `SELECT` that computes `plan` over `from`, keeping the rows
/// where all of the rendered predicates in `where` hold. Resolves each input
/// with `translate_field` in `columns`, and quotes aliases with `dialect`.
/// Returns `None` if `plan` has no columns, an input has no value, or
/// `aggregates` vetoes a column or an alias.
///
/// The query selects each result column under its alias and groups by the
/// values of the group keys. It yields one row per group, and without group
/// keys exactly one row, as `summarize` does.
auto make_aggregate_select(AggregationPlan const& plan,
                           ColumnModel const& columns,
                           SqlQueryDialect const& dialect,
                           SqlAggregateDialect const& aggregates,
                           std::string from, std::span<std::string const> where)
  -> Option<SqlSelect>;

} // namespace tenzir::pushdown
