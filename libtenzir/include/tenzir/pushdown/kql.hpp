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
#include "tenzir/pushdown/renderer.hpp"

#include <cstdint>
#include <string>
#include <string_view>

/// Pushdown into the Kusto Query Language (KQL), shared by the sources that
/// run a user's KQL query, such as Microsoft Defender advanced hunting and
/// Azure Log Analytics.
///
/// These sources know neither the columns nor the types of a query's output
/// before they run it, so every column is a `DynamicType`, and translation
/// yields prefilters only. The sources therefore evaluate every predicate
/// locally, and the KQL that this module renders only needs to keep a superset
/// of the rows that a predicate keeps. Each rule below states why it does.
///
/// The decoded values that TQL sees and the values that KQL compares differ in
/// three ways that all rules account for:
/// - A column that the query does not return is missing in TQL, where KQL
///   fails the whole query on an unknown name. Columns are therefore read with
///   `column_ifexists(name, dynamic(null))`, which yields `null` instead.
/// - Several KQL types decode to the same TQL type: `guid` and `decimal`
///   decode to strings, and a `dynamic` string that holds JSON decodes to the
///   JSON value, so that the dynamic string `"42"` may decode to the number
///   `42`. A rule compares a value in KQL only when KQL reports the type that
///   the literal requires, through `gettype()`, and keeps every other row.
/// - KQL is statically typed and rejects a comparison of mismatched types for
///   the whole query, while TQL yields `null` per row. Every comparison
///   converts its operand with a conversion function that accepts any type,
///   such as `tostring()` or `todouble()`, so that the query always compiles.
///
/// The rendered predicates are supersets, never exact translations, so they
/// must not be negated. `KqlRenderer` vetoes `Not`.
namespace tenzir::pushdown {

/// Spells IR prefilters in KQL.
///
/// Renders only expressions over `DynamicType` columns, as translated by
/// `translate_prefilter`, and guarantees that the rendered predicate is `true`
/// for every row where the IR predicate holds on the decoded values. Rows may
/// pass that the IR predicate drops.
///
/// Supported shapes, each a superset for the reason given in the source:
/// - `==`, `<`, `<=`, `>`, `>=` between a column and a number,
/// - `==` between a column and a string or a boolean,
/// - `in` of a column in a list of such literals of a single kind,
/// - `starts_with`, `ends_with`, and `contains` of a column and a string,
///   case-sensitively,
/// - `!= null` of a column,
/// - `and` and `or` of these, where `and` drops conjuncts that do not render.
///
/// Everything else is vetoed and stays local, notably negation, nested field
/// paths, membership in lists that mix kinds, ordering on strings, regular
/// expressions, case-insensitive matching, IP addresses and subnets, times,
/// arithmetic, and `== null`.
class KqlRenderer final : public Renderer {
public:
  auto render(Expr const& expr) const -> Option<std::string> override;
};

/// Returns whether a stage appended to `query` on a new line applies to the
/// rows that `query` returns, without changing whether the query compiles.
///
/// The check lexes `query` without parsing it. It rejects a query that it
/// cannot lex, such as one with an unterminated string, a management command,
/// a query that ends in `;` or `|`, a query that uses `render`, `fork`, or
/// `facet`, which must end a query or return several tables, and a query with
/// a `set` statement, which may change how the service limits a result.
auto kql_accepts_trailing_stages(std::string_view query) -> bool;

/// The query that a KQL source sends.
struct KqlPlan {
  /// The user's query, unchanged, followed by the appended stages.
  std::string query;
  /// Whether a `where` stage with prefilters, preceded by a `take` stage with
  /// any result cap, was appended.
  bool filtered = false;
  /// Whether a `project` stage was appended.
  bool projected = false;
  /// Whether a `take` stage was appended.
  bool limited = false;
};

/// Appends optimizer hints to a user's KQL `query` as trailing stages, in the
/// order `where`, `project`, and `take`, so that they apply to the rows that
/// `query` returns.
///
/// The `where` stage holds the prefilters of `filter` that render. The source
/// must still evaluate all of `filter` locally.
///
/// `result_cap` is the number of rows of `query` after which the service
/// silently truncates the result. If set, a `take result_cap` stage precedes
/// the `where` stage, so that the filter never reveals rows beyond that cap.
/// Pass `None` for a service that fails a truncated result instead: capping
/// before the filter would then turn that failure into a silently partial
/// result, while the uncapped filter either yields the complete filtered
/// result or still fails. The `project` stage and the `take` stage of a limit
/// need no cap: they keep the rows that `query` returns, or a prefix.
///
/// The
/// `project` stage keeps the columns that `projection` and `filter` reference,
/// and the `take` stage is appended only for an empty `filter`, since every
/// predicate stays local and would otherwise run after the limit, and only for
/// a limit between 1 and 2^31 - 1. The source must enforce `limit` locally in
/// any case. The planner never changes the request window: without parsing the
/// query, no predicate on its output provably refers to the time that the
/// window selects.
///
/// Returns `query` unchanged if `kql_accepts_trailing_stages` rejects it.
auto plan_kql_query(std::string_view query, ir::OptimizeFilter const& filter,
                    Option<ir::OptimizeProjection> const& projection,
                    Option<uint64_t> limit, Option<uint64_t> result_cap,
                    registry const& reg) -> KqlPlan;

} // namespace tenzir::pushdown
