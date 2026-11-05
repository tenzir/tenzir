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

auto is_identifier_start(char c) -> bool {
  return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}

auto is_identifier_char(char c) -> bool {
  return is_identifier_start(c) or (c >= '0' and c <= '9');
}

/// Returns the delimiter of the dollar-quoted string at `begin`, such as `$$`
/// or `$tag$`, or an empty view if none starts there. A `$` followed by a
/// digit is a parameter instead.
auto dollar_delimiter(std::string_view query, size_t begin)
  -> std::string_view {
  if (query[begin] != '$') {
    return {};
  }
  auto i = begin + 1;
  if (i < query.size() and is_identifier_start(query[i])) {
    while (i < query.size() and is_identifier_char(query[i])) {
      ++i;
    }
  }
  if (i < query.size() and query[i] == '$') {
    return query.substr(begin, i + 1 - begin);
  }
  return {};
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
    if (lexicon.triple_backtick_comments and rest.starts_with("```")) {
      auto const end = query.find("```", i + 3);
      result += "``` *** ```";
      i = end == std::string_view::npos ? query.size() : end + 3;
      continue;
    }
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
    if (lexicon.dollar_quoted_strings
        and (i == 0 or not is_identifier_char(query[i - 1]))) {
      if (auto delimiter = dollar_delimiter(query, i); not delimiter.empty()) {
        auto const end = query.find(delimiter, i + delimiter.size());
        result += delimiter;
        result += "***";
        result += delimiter;
        i = end == std::string_view::npos ? query.size()
                                          : end + delimiter.size();
        continue;
      }
    }
    if (lexicon.escape_strings and (c == 'E' or c == 'e') and rest.size() > 1
        and rest[1] == '\''
        and (i == 0 or not is_identifier_char(query[i - 1]))) {
      i = skip_quoted(query, i + 2, '\'', true);
      result += c;
      result += "'***'";
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
