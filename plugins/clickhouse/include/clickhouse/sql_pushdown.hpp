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
#include "tenzir/tql2/ast.hpp"

#include <map>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Translation of optimizer hints into ClickHouse SQL for `from_clickhouse`.
///
/// The optimizer hands `from_clickhouse` the entire filter chain that follows
/// it, plus an optional limit and projection. This module rewrites the
/// `SELECT` that the operator sends so that ClickHouse does as much of that
/// work as possible. Whatever cannot be expressed in SQL with the exact
/// semantics of TQL stays behind and is evaluated by the operator itself.
///
/// The translation is deliberately narrow: a predicate is only pushed when
/// both sides agree on the outcome for every possible row, including nulls
/// and type mismatches. Both TQL and SQL use three-valued logic for `and`,
/// `or`, and `not`, and both drop rows whose predicate is `null`, so boolean
/// combinators translate one-to-one. Comparisons are gated on the ClickHouse
/// column type: a comparison is pushed only when TQL would also compare
/// values of the same kind. Where TQL yields `null` (with a warning) for a
/// type mismatch, ClickHouse would attempt a conversion and fail the query.
///
/// TODO: Extend the translation to the following, each of which needs an
/// argument for why the semantics agree:
/// - `time` constants against `Date`/`DateTime`/`DateTime64` columns, taking
///   time zones and sub-second precision into account.
/// - `ip` and `subnet` constants against `IPv4`/`IPv6` columns, including
///   `in` with a subnet.
/// - `duration` constants against `IntervalX` columns.
/// - Ordering comparisons on strings, `FixedString`, `Enum`, `UUID`, `Decimal`,
///   and the 128/256-bit integer types.
/// - Comparisons between two columns.
/// - Arithmetic and functions with a direct ClickHouse counterpart.
/// - Nested projections that select a subset of tuple elements. Today, a
///   nested path in the projection selects its top-level column.
/// - Pushdown into a user-provided `sql` query, which requires parsing SQL.
namespace tenzir::plugins::clickhouse {

/// The value kinds that SQL pushdown compares. Each kind maps a family of
/// ClickHouse column types to the TQL constant types that compare against
/// them with identical semantics.
enum class SqlKind {
  /// `Bool` columns and `bool` constants.
  boolean,
  /// Signed and unsigned integers up to 64 bits.
  integer,
  /// `Float32` and `Float64`.
  floating,
  /// `String` columns and `string` constants.
  string,
};

/// A column together with the properties that decide how to compare it.
struct SqlColumn {
  SqlKind kind;
  /// Whether ClickHouse may return `NULL` for the column. Equality on a
  /// nullable column needs a null guard, because TQL treats `null` as a value
  /// (`null == 1` is `false`, `null != 1` is `true`) while SQL propagates it.
  bool nullable;
};

/// The columns of a ClickHouse table that SQL pushdown may reference.
class SqlSchema {
public:
  /// Registers a `DESCRIBE TABLE` row. Named tuple elements become nested
  /// paths, so `t Tuple(a Int64)` makes `t.a` addressable. `Nullable` and
  /// `LowCardinality` wrappers are transparent.
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

  /// Returns the column at `path`, or `None` if `path` does not name a column
  /// or the column's type has no unambiguous TQL counterpart.
  auto find(std::span<const std::string> path) const -> Option<SqlColumn>;

private:
  auto add_path(std::vector<std::string>& path, std::string_view type) -> void;

  std::vector<std::string> columns_;
  std::set<std::string, std::less<>> generated_;
  std::map<std::vector<std::string>, SqlColumn> columns_by_path_;
};

/// The outcome of splitting a filter chain into SQL and local parts.
struct SqlFilterSplit {
  /// Predicates rendered as SQL, to be joined with `AND`.
  std::vector<std::string> pushed;
  /// Predicates that the operator must evaluate itself, in order.
  ir::OptimizeFilter remaining;
};

/// Splits `filter` into predicates that `translate_predicate` accepts and the
/// rest. A predicate that is a conjunction is split into its conjuncts first,
/// so that `x > 0 and f(y)` pushes `x > 0` and keeps `f(y)`.
auto split_filter_for_sql(ir::OptimizeFilter filter, SqlSchema const& schema)
  -> SqlFilterSplit;

/// Translates `expr` into a SQL predicate, or returns `None` if some part of
/// it has no translation with identical semantics.
auto translate_predicate(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string>;

/// Renders the `SELECT` for `table`. `schema` is required when `projection`
/// is set; without a schema, the projection is ignored. Each path in the
/// projection selects its top-level column when that column exists; if no
/// column survives, or a path names a generated column, every column is
/// selected.
auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string;

/// Quotes `name` as a ClickHouse identifier using backticks.
auto quote_sql_identifier(std::string_view name) -> std::string;

/// Quotes `text` as a ClickHouse string literal using single quotes.
auto quote_sql_string(std::string_view text) -> std::string;

} // namespace tenzir::plugins::clickhouse
