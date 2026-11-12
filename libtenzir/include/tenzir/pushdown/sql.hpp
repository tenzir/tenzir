//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ip.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/expr_render.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/time.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

/// Assembly of the `SELECT` that a SQL source sends for a table.
///
/// In `table` mode, a SQL source reads a table with a `SELECT` that computes
/// the stages that it took over from the operators after it (see
/// `render_plan`). What SQL dialects disagree on, such as how to spell a limit
/// or a count, is up to the source's dialect.
namespace tenzir::pushdown {

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
  /// The terms to order by, each with its direction, as they appear after
  /// `ORDER BY`.
  std::vector<std::string> order_by;
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
///
/// Expressions spell as in standard SQL by default: `NULL`, `=` and `<>`,
/// `AND`, `OR`, `NOT`, `IS NULL`, and `IN (...)`.
class SqlQueryDialect : public ExpressionDialect {
public:
  /// Quotes an identifier, such as a column name or the alias of a result
  /// column.
  virtual auto quote_identifier(std::string_view name) const -> std::string = 0;

  /// Where the dialect spells a limit. Defaults to `LimitClause::limit`.
  virtual auto limit_clause() const -> LimitClause;

  /// The largest limit that the target accepts. A target that fails the query
  /// on a larger one overrides this. Defaults to the largest `uint64_t`.
  virtual auto max_limit() const -> uint64_t;

  /// Whether a table read that projects the table in a derived table, as in
  /// `SELECT ... FROM (SELECT a, b FROM t) AS ...`, evaluates the filters
  /// that directly follow the source in the `WHERE` of that derived table,
  /// next to the table itself. A target that analyzes only the `WHERE` that
  /// reads a table, for example to find the partitions to read, needs this.
  /// Defaults to `false`.
  virtual auto filters_in_projection() const -> bool;

  /// Spells a string literal, or returns `None` if the dialect cannot spell
  /// `text`. Defaults to single quotes, which doubles a quote inside.
  auto quote_string(std::string_view text) const
    -> Option<std::string> override;

  /// Spells a finite `double` so that the target reads it as a floating-point
  /// number, also where its value is integral, as in `2.0`. Defaults to the
  /// shortest decimal spelling with an exponent, such as `2e0` or `1.5e0`.
  auto render_double(double value) const -> std::string override;

  /// Spells a boolean literal. Defaults to `TRUE` or `FALSE`.
  auto render_bool(bool value) const -> std::string override;

  /// Whether a boolean value may stand where the dialect expects a predicate,
  /// as in `WHERE flag`, and a predicate where it expects a value, as in
  /// `(x > 1) = TRUE`. Defaults to `true`.
  auto boolean_values() const -> bool override;

  auto render_null() const -> Option<std::string> override;
  auto render_int(int64_t value) const -> Option<std::string> override;
  auto render_uint(uint64_t value) const -> Option<std::string> override;
  auto binary_operator(ast::binary_op op) const
    -> Option<std::string_view> override;
  auto render_not(std::string_view operand) const
    -> Option<std::string> override;
  auto render_negate(std::string_view operand) const
    -> Option<std::string> override;
  auto render_null_check(std::string_view operand, bool negated) const
    -> Option<std::string> override;
  auto render_in(std::string_view operand,
                 std::span<std::string const> elements) const
    -> Option<std::string> override;

  // -- Plans ------------------------------------------------------------------
  //
  // The hooks below let `render_plan` spell stages. Each defaults to vetoing,
  // except where noted, so that a dialect opts in to what it spells. Times,
  // durations, IP addresses, subnets, and calls spell through the hooks of
  // `ExpressionDialect`, which veto by default.

  /// Spells the field at `path` of a relation whose columns the plan does not
  /// list, such as a table. Defaults to quoting a single segment, and vetoes
  /// nested fields.
  virtual auto bind_field(std::span<std::string const> path) const
    -> Option<std::string>;

  /// Spells a call of the TQL aggregation function `name` on the rendered
  /// value of its argument, if any. Defaults to `COUNT`, `MIN`, and `MAX`.
  /// Only functions whose result does not depend on the order of their input
  /// qualify, since a plan does not order the input of an aggregation.
  virtual auto
  render_aggregate(std::string_view name, Option<std::string> const& arg) const
    -> Option<std::string>;

  /// Spells a selection of every column but the listed top-level ones, such
  /// as `* EXCLUDE (a, b)`.
  virtual auto render_exclude(std::span<std::string const> columns) const
    -> Option<std::string>;
};

/// Spells `select` in `dialect`.
auto render_select(SqlSelect const& select, SqlQueryDialect const& dialect)
  -> std::string;

/// The fields of a relation that a plan names, each with the column that
/// holds it.
using SqlFields = LayoutFields;

/// A relation that a plan reads.
struct SqlRelation {
  /// The relation, as it appears after `FROM`.
  std::string from;
  /// The fields of the relation with their columns, or `None` if the plan
  /// names its fields through `SqlQueryDialect::bind_field`, as for a table.
  Option<SqlFields> fields;
  /// The name that `from` already gives the relation, as in
  /// `(SELECT ...) AS name`, or `None` if it gives none. A plan that selects
  /// every column next to others qualifies the `*` with it, as some dialects
  /// require, and names the relation itself where `from` does not.
  Option<std::string> alias = None{};
};

/// How the result columns of a plan become events, as `apply_layout` arranges
/// them.
using SqlLayout = Layout;

/// The `SELECT` that computes a plan, and how its result becomes events.
struct SqlPlan {
  SqlSelect select;
  SqlLayout layout;
  /// The names of the columns that the query introduces, all starting with
  /// `__tenzir_`. The names of relations, which may share their name with a
  /// column, are not among them. The source must decline the plan if one of
  /// them names a column of the relation, compared as the target compares
  /// identifiers, such as without regard to case in DuckDB.
  std::vector<std::string> aliases;
};

/// Assembles a `SELECT` that computes `plan` over `relation` with the
/// semantics of the target, or returns `None` if `dialect` cannot spell some
/// stage. It decides from the syntax of the stages and the fields of the
/// relation alone, so that a source can decide what to take over before it
/// knows the types of the columns.
///
/// Each stage joins the current `SELECT` where SQL's order of clauses allows
/// it, and wraps the query in a derived table where not, such as for a filter
/// after an aggregation or a sort after a limit. An order that a sort
/// establishes survives the wrapping: the derived table keeps the sort keys in
/// hidden columns, and the outer query orders by them.
///
/// Filters spell field paths of the input, constants of type `null`, `bool`,
/// `int64`, `uint64`, finite `double`, and `string`, and, where the dialect
/// spells them, times, durations, IP addresses, and subnets, together with
/// comparisons, `and`, `or`, `not`, arithmetic, negation, `== null`,
/// `!= null`, `in` with a list of constants, and calls that the dialect
/// spells. Sorts order by fields. Limits fit into `dialect.max_limit()`.
auto render_plan(SqlRelation relation, std::span<ir::QueryStage const> plan,
                 SqlQueryDialect const& dialect) -> Option<SqlPlan>;

} // namespace tenzir::pushdown
