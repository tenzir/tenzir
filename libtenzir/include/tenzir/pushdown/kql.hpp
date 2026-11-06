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
#include "tenzir/pushdown/layout.hpp"

#include <span>
#include <string>
#include <string_view>

/// The queries in the Kusto Query Language (KQL) that the sources send that
/// run a user's KQL query, such as Microsoft Defender advanced hunting and
/// Azure Log Analytics, with the stages that they take over.
///
/// A source appends the stages that it takes over to the user's query, each
/// on a new line, where they run with KQL's semantics:
///
/// - A filter becomes `where`. Strings compare case-sensitively, a string is
///   never `null` but may be empty, IP addresses and subnets are strings that
///   compare as text, and a comparison of mismatched types either fails the
///   query or holds for no row.
/// - A select becomes `project`, and a drop `project-away`, which fails the
///   query for a column that it does not return.
/// - A summary becomes `summarize`, which keeps a group for `null` keys.
/// - A sort becomes `sort` with an explicit direction, which orders `null`
///   first ascending and last descending, and a limit `take`.
///
/// The plan decides from the syntax of the stages alone. A type that KQL
/// rejects, such as a sum of strings or a group key of type `dynamic`, fails
/// the query with the service's error.
namespace tenzir::pushdown {

/// Returns whether a stage appended to `query` on a new line applies to the
/// only table that `query` returns, without changing whether the query
/// compiles.
///
/// The check lexes `query` without parsing it. It rejects a query that it
/// cannot lex, such as one with an unterminated string or unbalanced brackets,
/// a management command, a query that ends in `;` or `|`, and a query that
/// uses `render`, `fork`, or `facet`, which must end a query or return several
/// tables, or a `set` statement, which may change how the service limits a
/// result. Of the statements of `query`, separated by `;`, only the last may
/// be tabular: every other one must be a `let`, `declare`, or `alias`
/// statement, since each tabular statement returns a table of its own.
auto kql_accepts_trailing_stages(std::string_view query) -> bool;

/// The largest number of bytes that a KQL source appends to a user's query.
/// Both services limit the size of a query; stages beyond this budget stay in
/// the pipeline.
inline constexpr auto max_kql_appendix = size_t{16'384};

/// The query that a KQL source sends, and how its result becomes events.
struct KqlQuery {
  /// The query to send: the user's query, followed by `generated`.
  std::string query;
  /// The text that the source appends to the user's query.
  std::string generated;
  /// How the result rows become events.
  Layout layout;
};

/// Spells the query that runs the user's `query` and computes `stages` after
/// it, or returns `None` if KQL does not spell some stage, if the stages
/// exceed `max_kql_appendix`, or if `query` does not accept stages after it.
///
/// Without a stage that enumerates its output, the query ends with a
/// `project` that keeps the top-level fields of `projection`, except those that
/// a drop removed, as long as it fits into `max_kql_appendix` with the stages.
/// A projected field that the query does not return becomes
/// `null`, while a returned column keeps its type. Without a projection, or
/// with one that refers to a whole event, the query keeps every column. A query
/// that does not accept stages goes out unchanged, if there are none.
auto plan_kql_query(std::string_view query,
                    std::span<ir::QueryStage const> stages,
                    Option<ir::OptimizeProjection> const& projection)
  -> Option<KqlQuery>;

} // namespace tenzir::pushdown
