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
#include "tenzir/pushdown/pipe_plan.hpp"
#include "tenzir/pushdown/plan.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The searches in the Search Processing Language (SPL) that `from_splunk`
/// sends, with the stages that it takes over.
///
/// `from_splunk` appends the stages that it takes over to the search, where
/// they run with Splunk's semantics:
///
/// - With `index`, the conjuncts of the leading filters that the `search`
///   command spells extend the base search, which matches strings without
///   regard to case and with wildcards, and networks by CIDR. Other filters
///   become `where` stages, which compare exactly.
/// - A select keeps fields with `fields`, after an `eval` that aliases fields
///   that a command cannot name.
/// - A drop removes fields with `fields -`.
/// - A summary becomes `stats`, which drops results whose group key is
///   `null` and counts each value of a multivalue field.
/// - A sort becomes `sort` with an explicit count, and a limit `head`.
///
/// Splunk returns every value as a string, which `from_splunk` decodes.
namespace tenzir::pushdown {

/// How SPL delimits string literals and comments, for `redact_query`. Single
/// quotes delimit field names in `eval`, which then redact like strings.
inline constexpr auto spl_lexicon = SqlLexicon{
  .backslash_escapes = true,
  .double_quoted_strings = true,
  .bracket_identifiers = false,
  .identifier_backslash_escapes = false,
  .hash_comments = false,
  .escape_strings = false,
  .dollar_quoted_strings = false,
  .triple_backtick_comments = true,
};

/// What appending stages to an SPL search allows.
struct SplStages {
  /// Whether stages appended to the search apply to its results without
  /// changing whether the search parses.
  bool accepted = false;
  /// Whether a `head` stage may follow the search. Splunk could then stop the
  /// search early, so a search with a command that has side effects, such as
  /// `outputlookup` or `collect`, or with a macro, which may hide one, gets
  /// none. This does not keep the search running once the pipeline ends, for
  /// example after a downstream `head`: closing the request may end it too.
  bool limitable = false;
  /// Whether the search consists of a single `search` command without macros,
  /// whose results are events with Splunk's own fields, such as `_time`. A
  /// command after it, or a macro, may rewrite any field, as
  /// `| rename event* AS _*` does.
  bool plain = false;
  /// Whether the search formats the values of its results with `fieldformat`,
  /// or has a macro, which may hide it. The results then show the formatted
  /// values, but a stage after the search, such as `where` or `stats`, sees
  /// the original ones.
  bool formatted = false;
};

/// Prepends the `search` command to a search that starts with neither a
/// command nor `|`, as Splunk Web does, so that a search such as `index=main`
/// runs through the REST API as well. Other searches stay unchanged.
auto spl_search_command(std::string_view search) -> std::string;

/// Checks whether stages can be appended to the SPL `search`.
///
/// The check lexes the search without parsing it. It rejects a search that it
/// cannot lex, such as one with an unterminated string, comment, or subsearch,
/// and a search that is empty or ends in `|`.
auto spl_stages(std::string_view search) -> SplStages;

/// Spells the search for the events of `indexes`, which must not be empty, or
/// returns `None` if an index name has no spelling.
auto spl_index_search(std::span<std::string const> indexes)
  -> Option<std::string>;

/// What `from_splunk` searches: the user's search, or the events of indexes.
struct SplSource {
  /// The user's search, as it runs through the REST API, or `None` for
  /// `indexes`.
  Option<std::string> search;
  /// The indexes whose events to search, if there is no search.
  std::vector<std::string> indexes;
};

/// Returns why `from_splunk` takes over no stage after `source`, or `None` if
/// it may. A user's search must accept stages after it and must not format
/// its values.
auto spl_declines(SplSource const& source) -> Option<std::string>;

/// The search that `from_splunk` sends, and how its results become events.
struct SplQuery {
  /// The search to send.
  std::string search;
  /// The part of `search` that `from_splunk` generated: the stages after a
  /// user's search, or the whole search for indexes.
  std::string generated;
  /// How the result rows become events, after removing the sentinel.
  Layout layout;
  /// The field of the last row, which counts the rows before it, if the
  /// search ends with one. A search that fails in `eval` or `stats` returns
  /// no results and no error, so the row proves that the search ran.
  Option<std::string> sentinel;
  /// The result fields that hold counts, which decode as integers also
  /// without type inference.
  std::vector<std::string> counts;
  /// The time window of the events that pass the leading filters, for the
  /// time bounds of the request. Only searches for indexes have one.
  TimeWindow window;
};

/// Spells the search that reads `source` and computes `stages` after it, or
/// returns `None` if Splunk does not spell some stage. Without stages that
/// enumerate their output, the search ends with `fields`, which keeps the
/// top-level fields of `projection`, or every field without one, since
/// Splunk's export endpoint returns only the fields that a search references.
/// A search with an aggregation ends with the sentinel row.
///
/// A user's search that `spl_declines` rejects takes no stages, and a user's
/// search that gets none goes out unchanged if it does not accept them. A
/// limit needs a search that `spl_stages` finds limitable.
auto plan_spl_query(SplSource const& source,
                    std::span<ir::QueryStage const> stages,
                    Option<ir::OptimizeProjection> const& projection)
  -> Option<SplQuery>;

/// Parses a count that Splunk renders as decimal digits.
auto parse_spl_count(std::string_view text) -> Option<int64_t>;

} // namespace tenzir::pushdown
