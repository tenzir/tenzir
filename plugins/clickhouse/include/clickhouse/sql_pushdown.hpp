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
#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

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
/// Literals are always rendered in the column's own ClickHouse type, so that
/// ClickHouse never converts the column. Its implicit conversions are where
/// the semantics drift: `Date` to `DateTime64` goes through the session time
/// zone, `DateTime64(3)` to `DateTime64(9)` overflows for years past 2262,
/// and `IPv4` in a set of `IPv6` fails outright. A literal that the column's
/// type cannot hold (a sub-second time against a `DateTime`, an IPv6 address
/// against an `IPv4` column) is decided at translation time instead.
///
/// TODO: Extend the translation to the following, each of which needs an
/// argument for why the semantics agree:
/// - A typed SQL intermediate representation between translation and
///   rendering, so that the semantic reasoning here can be shared with other
///   SQL sources and the spelling lives in a per-dialect renderer (TNZ-1070).
/// - Pushdown into a user-provided `sql` query, which requires parsing SQL.
/// - Ordering on `Enum`, `UUID`, `Decimal`, and the 128-bit integers. TQL
///   sees these as strings and orders them as text, while ClickHouse orders
///   them by their numeric value, so there is no direct translation.
/// - Arithmetic on 64-bit integer columns and between two columns. TQL yields
///   `null` on overflow where ClickHouse wraps around, and without a bound on
///   the operands there is no way to rule overflow out.
namespace tenzir::plugins::clickhouse {

/// `Bool` columns. TQL sees `bool`.
struct SqlBool {};

/// Signed and unsigned integers up to 64 bits. TQL sees `int64` or `uint64`.
struct SqlInt {
  /// The width in bits, which bounds the result of arithmetic.
  uint8_t bits;
  bool is_signed;
};

/// `Float32` and `Float64`. TQL sees `double`.
struct SqlFloat {};

/// `String`. TQL sees `string`.
struct SqlString {};

/// `FixedString(N)`. TQL sees all `N` bytes, including the zero padding.
struct SqlFixedString {
  size_t length;
};

/// `Enum8` and `Enum16`. TQL sees the element names.
struct SqlEnum {
  detail::flat_set<std::string> names;
};

/// `UUID`. TQL sees the canonical lowercase text.
struct SqlUuid {};

/// `Date`, `Date32`, `DateTime`, and `DateTime64`. TQL sees `time`.
struct SqlTime {
  enum class Family { date, date32, datetime, datetime64 };
  Family family;
  /// The number of fractional digits of a `datetime64`.
  uint8_t precision = 0;
};

/// `IPv4` and `IPv6`. TQL sees `ip`.
struct SqlIp {
  bool v4;
};

/// The column types that SQL pushdown compares. Each maps a family of
/// ClickHouse column types to the TQL values that `from_clickhouse` decodes
/// them into.
using SqlType = variant<SqlBool, SqlInt, SqlFloat, SqlString, SqlFixedString,
                        SqlEnum, SqlUuid, SqlTime, SqlIp>;

/// A column together with the properties that decide how to compare it.
struct SqlColumn {
  SqlType type;
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

  /// Returns the column at `path`, or `None` if `path` does not name a column
  /// or the column's type has no unambiguous TQL counterpart.
  auto find(std::span<const std::string> path) const -> Option<SqlColumn>;

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
  /// type is a leaf, comparable if `column` is set.
  struct Node {
    /// The ClickHouse type text, with insignificant whitespace removed.
    std::string type;
    Option<SqlColumn> column;
    /// Named tuple elements in declaration order. Boxed because the type
    /// recurses through the map's value type.
    detail::stable_map<std::string, Box<Node>> children;
  };

  static auto make_node(std::string_view type) -> Node;

  auto find_node(std::span<const std::string> path) const -> Node const*;

  std::vector<std::string> columns_;
  detail::flat_set<std::string> generated_;
  detail::stable_map<std::string, Node> nodes_;
};

/// The outcome of splitting a filter chain into SQL and local parts.
struct SqlFilterSplit {
  /// Predicates rendered as SQL, to be joined with `AND`. A predicate without
  /// an exact translation may still contribute a prefilter here, in which case
  /// it also stays in `remaining`.
  std::vector<std::string> pushed;
  /// Predicates that the operator must evaluate itself, in order.
  ir::OptimizeFilter remaining;
};

/// Splits `filter` into predicates that `translate_predicate` accepts and the
/// rest, after adapting each to the schema. A predicate that is a conjunction
/// is split into its conjuncts first, so that `x > 0 and f(y)` pushes `x > 0`
/// and keeps `f(y)`. A remaining conjunct with a `translate_prefilter` result
/// pushes that prefilter in addition to staying local.
auto split_filter_for_sql(ir::OptimizeFilter filter, SqlSchema const& schema)
  -> SqlFilterSplit;

/// Adapts `expr` in place to the ClickHouse types of the columns it references.
///
/// Tables frequently store IP addresses as `String`. TQL compares a string
/// with an `ip` as a type mismatch, which yields `null` for every row, so a
/// user who writes `src == 1.1.1.1` against such a column gets no rows and a
/// warning. The bridge to ClickHouse resolves this by the column's type
/// instead: an `ip` literal compared for equality or membership with a
/// `String` column becomes its canonical text, so `src == 1.1.1.1` reads
/// `src == "1.1.1.1"`, and `src in 10.0.0.0/8` parses the column with
/// `src.ip()`. The adapted expression is what the operator evaluates locally
/// and what it translates, so the result does not depend on how much of it
/// was pushed. Ordering comparisons are left alone, because the textual order
/// of addresses is not their numeric order.
auto adapt_to_schema(ast::expression& expr, SqlSchema const& schema) -> void;

/// Translates `expr` into a SQL predicate, or returns `None` if some part of
/// it has no translation with identical semantics.
auto translate_predicate(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string>;

/// Translates `expr` into a SQL predicate that holds for every row where
/// `expr` is `true`, or returns `None` if there is none beyond `true` itself.
///
/// A prefilter lets ClickHouse drop rows that the local evaluation of `expr`
/// would drop anyway, while `expr` itself stays in the pipeline to decide the
/// rest. This covers predicates whose exact translation would require two
/// parsers to agree, such as `src.ip() in 10.0.0.0/8` on a `String` column:
/// ClickHouse parses the column with `toIPv6OrNull`, and the prefilter keeps
/// every row that it cannot parse, so it only relies on the two parsers
/// agreeing on the strings that both of them accept. Supersets compose:
/// `and` takes the prefilters of the conjuncts that have one, `or` needs one
/// for each side.
auto translate_prefilter(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string>;

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
