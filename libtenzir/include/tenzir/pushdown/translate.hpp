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
#include "tenzir/tql2/ast.hpp"

#include <string>
#include <vector>

/// Translation of optimizer filters into the pushdown IR.
///
/// The optimizer hands a source operator the entire filter chain that follows
/// it. This module translates as much of that chain as possible into IR
/// expressions, which the source renders into the query it sends, so that the
/// target does the work. Whatever has no translation that keeps and drops the
/// same rows as TQL stays behind and is evaluated by the operator itself.
///
/// The translation is deliberately narrow: a predicate is only pushed when
/// both sides agree on whether to keep each possible row, including rows with
/// nulls and type mismatches. Both TQL and the IR use three-valued logic for
/// `and`, `or`, and `not`, and both drop rows whose predicate is `null`, so
/// boolean combinators translate one-to-one. Comparisons are gated on the
/// column type: a comparison is pushed only when TQL would also compare values
/// of the same kind. Where TQL yields `null` (with a warning) for a type
/// mismatch, a target would attempt a conversion or fail the query.
///
/// One rule trades exactness for coverage: matching that ignores case, as in
/// `starts_with(x, "a", ignore_case=true)`, uses the target's notion of case
/// (see `Operation::fold_case`). It agrees with TQL on ASCII, but a target may
/// not match `Straße` against `strasse`, where TQL does.
///
/// The rules do not model how each target compares mixed numbers, handles
/// integer overflow, or divides by zero. They are conservative bounds that
/// hold whether a target throws, wraps, or promotes: a predicate is pushed
/// only where all of these agree with TQL. A more permissive target thus gets
/// less pushed, never a different result.
///
/// Literals compared with temporal and IP columns are always in the column's
/// own type, so that the target never converts the column. Implicit
/// conversions are where the semantics drift: in ClickHouse, for example,
/// `Date` to `DateTime64` goes through the session time zone, `DateTime64(3)`
/// to `DateTime64(9)` overflows for years past 2262, and `IPv4` in a set of
/// `IPv6` fails outright. A literal that the column's type cannot hold (a
/// sub-second time against a `DateTime`, an IPv6 address against an `IPv4`
/// column) is decided at translation time instead.
///
/// TODO: Extend the translation to the following, each of which needs an
/// argument for why the semantics agree:
/// - Ordering on enums, UUIDs, decimals, and 128-bit integers. TQL sees these
///   as strings and orders them as text, while a target orders them by their
///   numeric value, so there is no direct translation.
/// - Arithmetic on 64-bit integer columns and between two columns. TQL yields
///   `null` on overflow where a target may wrap around, and without a bound on
///   the operands there is no way to rule overflow out.
namespace tenzir::pushdown {

/// Adapts `expr` in place to the types of the columns it references.
///
/// Tables frequently store IP addresses as strings. TQL compares a string
/// with an `ip` as a type mismatch, which yields `null` for every row, so a
/// user who writes `src == 1.1.1.1` against such a column gets no rows and a
/// warning. The bridge to the target resolves this by the column's type
/// instead: an `ip` literal compared for equality or membership with a string
/// column becomes its canonical text, so `src == 1.1.1.1` reads
/// `src == "1.1.1.1"`, and `src in 10.0.0.0/8` parses the column with
/// `src.ip()`. The adapted expression is what the operator evaluates locally
/// and what it translates, so the result does not depend on how much of it
/// was pushed. Ordering comparisons are left alone, because the textual order
/// of addresses is not their numeric order.
auto adapt_to_columns(ast::expression& expr, ColumnModel const& columns)
  -> void;

/// Translates `expr` into an IR predicate that keeps and drops the same rows,
/// or returns `None` if some part of it has no such translation.
///
/// The result has the same keep-or-drop outcome as `expr`, not necessarily
/// identical semantics: since a filter drops a row on `false` and `null`
/// alike, a guard may turn a `null` of TQL into `false`. The result is thus
/// fit to filter by, also as an operand of `and` or `or`, but not to negate.
auto translate_predicate(ast::expression const& expr,
                         ColumnModel const& columns) -> Option<Expr>;

/// Translates `expr` into an IR predicate that holds for every row where
/// `expr` is `true`, or returns `None` if there is none beyond `true` itself.
///
/// A prefilter lets the target drop rows that the local evaluation of `expr`
/// would drop anyway, while `expr` itself stays in the pipeline to decide the
/// rest. This covers predicates whose exact translation would require two
/// parsers to agree, such as `src.ip() in 10.0.0.0/8` on a string column:
/// the target parses the column with `parse_ip`, and the prefilter keeps
/// every row that it cannot parse, so it only relies on the two parsers
/// agreeing on the strings that both of them accept. Supersets compose:
/// `and` takes the prefilters of the conjuncts that have one, `or` needs one
/// for each side.
auto translate_prefilter(ast::expression const& expr,
                         ColumnModel const& columns) -> Option<Expr>;

/// The outcome of splitting a filter chain into pushed and local parts.
struct FilterSplit {
  /// Rendered predicates, to be joined with `and`. A predicate without an
  /// exact translation may still contribute a prefilter here, in which case it
  /// also stays in `remaining`.
  std::vector<std::string> pushed;
  /// Predicates that the operator must evaluate itself, in order.
  ir::OptimizeFilter remaining;
};

/// Splits `filter` into predicates that translate and render, and the rest,
/// after adapting each to the columns. A predicate that is a conjunction is
/// split into its conjuncts first, so that `x > 0 and f(y)` pushes `x > 0` and
/// keeps `f(y)`. A remaining conjunct whose prefilter renders pushes that
/// prefilter in addition to staying local.
auto split_filter(ir::OptimizeFilter filter, ColumnModel const& columns,
                  Renderer const& renderer) -> FilterSplit;

} // namespace tenzir::pushdown
