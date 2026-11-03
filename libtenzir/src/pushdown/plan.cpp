//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/plan.hpp"

namespace tenzir::pushdown {

namespace {

/// Returns the position after the token that `close` ends, which starts after
/// `begin`. A doubled `close` stands for itself, and with `backslash`, so does
/// a character after a backslash. Returns the size of `query` if the token
/// does not end.
auto skip_quoted(std::string_view query, size_t begin, char close,
                 bool backslash) -> size_t {
  auto i = begin;
  while (i < query.size()) {
    if (backslash and query[i] == '\\' and i + 1 < query.size()) {
      i += 2;
      continue;
    }
    if (query[i] == close) {
      if (i + 1 < query.size() and query[i + 1] == close) {
        i += 2;
        continue;
      }
      return i + 1;
    }
    ++i;
  }
  return query.size();
}

} // namespace

auto redact_query(std::string_view query, SqlLexicon lexicon) -> std::string {
  auto result = std::string{};
  result.reserve(query.size());
  auto i = size_t{0};
  while (i < query.size()) {
    auto const c = query[i];
    auto const rest = query.substr(i);
    // A comment may hold anything, including quotes, so it goes as a whole.
    if (rest.starts_with("--") or (c == '#' and lexicon.hash_comments)) {
      auto const end = query.find('\n', i);
      result += rest.starts_with("--") ? "-- ***" : "# ***";
      i = end == std::string_view::npos ? query.size() : end;
      continue;
    }
    if (rest.starts_with("/*")) {
      // Some dialects nest block comments. Counting nesting everywhere only
      // redacts more in the others.
      auto depth = size_t{1};
      i += 2;
      while (i < query.size() and depth > 0) {
        auto const tail = query.substr(i);
        if (tail.starts_with("/*")) {
          ++depth;
          i += 2;
        } else if (tail.starts_with("*/")) {
          --depth;
          i += 2;
        } else {
          ++i;
        }
      }
      result += "/* *** */";
      continue;
    }
    auto const string
      = c == '\'' or (c == '"' and lexicon.double_quoted_strings);
    if (string) {
      i = skip_quoted(query, i + 1, c, lexicon.backslash_escapes);
      result += c;
      result += "***";
      result += c;
      continue;
    }
    auto close = char{0};
    if (c == '"' or c == '`') {
      close = c;
    } else if (c == '[' and lexicon.bracket_identifiers) {
      close = ']';
    }
    if (close != 0) {
      auto const end = skip_quoted(query, i + 1, close,
                                   lexicon.identifier_backslash_escapes);
      result.append(query.substr(i, end - i));
      i = end;
      continue;
    }
    result += c;
    ++i;
  }
  return result;
}

} // namespace tenzir::pushdown
