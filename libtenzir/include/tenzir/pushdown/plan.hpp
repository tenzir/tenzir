//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/location.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/option.hpp"

#include <string>
#include <string_view>

/// Shared pieces of sources that take over the stages behind them.
///
/// A source that computes stages with the semantics of its target, as
/// `Describer::query_plan` describes, offers the `pushdown` argument, which
/// turns that off, and logs the queries that it sends without their literals.
namespace tenzir::pushdown {

/// Adds the argument `pushdown=bool` to a source. With `pushdown=false`, the
/// source takes over no stage, so that they run in Tenzir with the semantics
/// of TQL. Explicit arguments of the source, such as a query or a time window,
/// stay in effect.
template <class Args, class... Impls>
auto add_pushdown_argument(Describer<Args, Impls...>& d,
                           Option<located<bool>> Args::* ptr) -> void {
  d.named("pushdown", ptr);
}

/// Returns whether a source with the `pushdown` argument `arg` may take over
/// stages.
inline auto pushdown_enabled(Option<located<bool>> const& arg) -> bool {
  return not arg or arg->inner;
}

/// How a SQL dialect delimits string literals and quoted identifiers, as far
/// as `redact_query` needs to know.
struct SqlLexicon {
  /// Whether a backslash escapes the next character in a string literal, as
  /// in ClickHouse and MySQL.
  bool backslash_escapes = false;
  /// Whether double quotes delimit string literals rather than identifiers,
  /// as in MySQL without `ANSI_QUOTES`.
  bool double_quoted_strings = false;
  /// Whether square brackets delimit identifiers, as in SQL Server.
  bool bracket_identifiers = false;
  /// Whether a backslash escapes the next character in a quoted identifier,
  /// as in ClickHouse.
  bool identifier_backslash_escapes = false;
  /// Whether `#` starts a comment until the end of the line, as in MySQL.
  bool hash_comments = false;
};

/// Returns `query` with the contents of its string literals and comments
/// replaced by `***`, for logs. Single quotes delimit a literal, which spells a
/// quote inside doubled, or after a backslash where `lexicon` says so. `--`
/// and `/*` start comments, and block comments nest. Quoted identifiers stay
/// as they are. An unterminated literal or comment is redacted to the end.
auto redact_query(std::string_view query, SqlLexicon lexicon = {})
  -> std::string;

} // namespace tenzir::pushdown
