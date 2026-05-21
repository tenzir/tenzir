//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_predicate_pushdown.hpp"

#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

#include <cctype>
#include <string>
#include <vector>

namespace tenzir::plugins::clickhouse {

namespace {

struct Token {
  std::string_view text;
  size_t begin = 0;
  size_t end = 0;
  size_t depth = 0;
};

auto is_identifier_char(char c) -> bool {
  return std::isalnum(static_cast<unsigned char>(c)) or c == '_';
}

auto equals_ci(std::string_view lhs, std::string_view rhs) -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < lhs.size(); ++i) {
    auto a
      = static_cast<char>(std::tolower(static_cast<unsigned char>(lhs[i])));
    auto b
      = static_cast<char>(std::tolower(static_cast<unsigned char>(rhs[i])));
    if (a != b) {
      return false;
    }
  }
  return true;
}

auto skip_single_quoted(std::string_view sql, size_t& i) -> bool {
  if (sql[i] != '\'') {
    return false;
  }
  ++i;
  while (i < sql.size()) {
    if (sql[i] == '\\') {
      i += i + 1 < sql.size() ? 2 : 1;
      continue;
    }
    if (sql[i] == '\'') {
      if (i + 1 < sql.size() and sql[i + 1] == '\'') {
        i += 2;
        continue;
      }
      ++i;
      return true;
    }
    ++i;
  }
  return false;
}

auto skip_quoted_identifier(std::string_view sql, size_t& i) -> bool {
  auto quote = sql[i];
  if (quote != '"' and quote != '`') {
    return false;
  }
  ++i;
  while (i < sql.size()) {
    if (sql[i] == quote) {
      if (i + 1 < sql.size() and sql[i + 1] == quote) {
        i += 2;
        continue;
      }
      ++i;
      return true;
    }
    ++i;
  }
  return false;
}

auto skip_comment(std::string_view sql, size_t& i) -> bool {
  if (sql[i] == '#') {
    i = sql.find('\n', i);
    if (i == std::string_view::npos) {
      i = sql.size();
    }
    return true;
  }
  if (sql[i] == '-' and i + 1 < sql.size() and sql[i + 1] == '-') {
    i = sql.find('\n', i + 2);
    if (i == std::string_view::npos) {
      i = sql.size();
    }
    return true;
  }
  if (sql[i] == '/' and i + 1 < sql.size() and sql[i + 1] == '*') {
    auto end = sql.find("*/", i + 2);
    if (end == std::string_view::npos) {
      return false;
    }
    i = end + 2;
    return true;
  }
  return false;
}

auto scan_tokens(std::string_view sql) -> Option<std::vector<Token>> {
  auto result = std::vector<Token>{};
  auto depth = size_t{0};
  auto saw_top_level_semicolon = false;
  auto i = size_t{0};
  while (i < sql.size()) {
    if (std::isspace(static_cast<unsigned char>(sql[i]))) {
      ++i;
      continue;
    }
    if (skip_comment(sql, i) or skip_single_quoted(sql, i)
        or skip_quoted_identifier(sql, i)) {
      continue;
    }
    if (sql[i] == '(') {
      ++depth;
      ++i;
      continue;
    }
    if (sql[i] == ')') {
      if (depth == 0) {
        return None{};
      }
      --depth;
      ++i;
      continue;
    }
    if (sql[i] == ';' and depth == 0) {
      saw_top_level_semicolon = true;
      ++i;
      while (i < sql.size()) {
        if (std::isspace(static_cast<unsigned char>(sql[i]))) {
          ++i;
          continue;
        }
        return None{};
      }
      break;
    }
    if (saw_top_level_semicolon) {
      return None{};
    }
    if (is_identifier_char(sql[i])) {
      auto begin = i;
      ++i;
      while (i < sql.size() and is_identifier_char(sql[i])) {
        ++i;
      }
      result.push_back(Token{
        .text = sql.substr(begin, i - begin),
        .begin = begin,
        .end = i,
        .depth = depth,
      });
      continue;
    }
    ++i;
  }
  if (depth != 0) {
    return None{};
  }
  return result;
}

auto is_top_level_clause(Token const& token) -> bool {
  if (token.depth != 0) {
    return false;
  }
  return equals_ci(token.text, "prewhere") or equals_ci(token.text, "where")
         or equals_ci(token.text, "group") or equals_ci(token.text, "having")
         or equals_ci(token.text, "window") or equals_ci(token.text, "order")
         or equals_ci(token.text, "limit") or equals_ci(token.text, "offset")
         or equals_ci(token.text, "fetch") or equals_ci(token.text, "settings")
         or equals_ci(token.text, "format");
}

auto is_top_level_set_operator(Token const& token) -> bool {
  if (token.depth != 0) {
    return false;
  }
  return equals_ci(token.text, "union") or equals_ci(token.text, "intersect")
         or equals_ci(token.text, "except");
}

auto token_index(std::vector<Token> const& tokens, std::string_view keyword)
  -> Option<size_t> {
  for (auto i = size_t{0}; i < tokens.size(); ++i) {
    if (tokens[i].depth == 0 and equals_ci(tokens[i].text, keyword)) {
      return i;
    }
  }
  return None{};
}

auto find_insert_position(std::vector<Token> const& tokens,
                          size_t after_token_index) -> size_t {
  for (auto i = after_token_index + 1; i < tokens.size(); ++i) {
    if (is_top_level_clause(tokens[i])) {
      return tokens[i].begin;
    }
  }
  return std::string_view::npos;
}

auto append_position(std::string_view sql) -> size_t {
  auto result = size_t{0};
  auto i = size_t{0};
  while (i < sql.size()) {
    if (std::isspace(static_cast<unsigned char>(sql[i]))) {
      ++i;
      continue;
    }
    auto begin = i;
    if (skip_comment(sql, i)) {
      continue;
    }
    if (skip_single_quoted(sql, i) or skip_quoted_identifier(sql, i)) {
      result = i;
      continue;
    }
    ++i;
    result = begin + 1;
  }
  return result;
}

auto insert_before(std::string_view sql, size_t pos, std::string_view text)
  -> std::string {
  auto result = std::string{};
  if (pos == std::string_view::npos) {
    result.reserve(sql.size() + text.size() + 1);
    result.append(sql);
    result.push_back(' ');
    result.append(text);
    return result;
  }
  auto trimmed_prefix = detail::trim_back(sql.substr(0, pos));
  result.reserve(sql.size() + text.size() + 2);
  result.append(trimmed_prefix);
  result.push_back(' ');
  result.append(text);
  result.push_back(' ');
  result.append(sql.substr(pos));
  return result;
}

} // namespace

auto pushdown_predicate_into_sql(std::string_view sql,
                                 std::string_view predicate)
  -> Option<std::string> {
  sql = detail::trim(sql);
  if (sql.ends_with(';')) {
    sql.remove_suffix(1);
    sql = detail::trim_back(sql);
  }
  if (predicate.empty()) {
    return std::string{sql};
  }
  auto tokens = scan_tokens(sql);
  if (not tokens or tokens->empty()) {
    return None{};
  }
  for (auto const& token : *tokens) {
    if (is_top_level_set_operator(token)) {
      return None{};
    }
  }
  auto select = token_index(*tokens, "select");
  if (not select or *select != 0) {
    return None{};
  }
  auto from = token_index(*tokens, "from");
  if (not from or *from <= *select) {
    return None{};
  }
  auto where = token_index(*tokens, "where");
  if (where and *where < *from) {
    return None{};
  }
  if (where) {
    auto insert_pos = find_insert_position(*tokens, *where);
    if (insert_pos == std::string_view::npos) {
      insert_pos = append_position(sql);
    }
    return insert_before(sql, insert_pos, fmt::format("AND ({})", predicate));
  }
  auto prewhere = token_index(*tokens, "prewhere");
  if (prewhere and *prewhere < *from) {
    return None{};
  }
  auto insert_pos = prewhere ? find_insert_position(*tokens, *prewhere)
                             : find_insert_position(*tokens, *from);
  if (insert_pos == std::string_view::npos) {
    insert_pos = append_position(sql);
  }
  return insert_before(sql, insert_pos, fmt::format("WHERE {}", predicate));
}

} // namespace tenzir::plugins::clickhouse
