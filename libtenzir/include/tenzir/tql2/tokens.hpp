//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <tenzir/detail/default_formatter.hpp>
#include <tenzir/detail/enum.hpp>

#include <fmt/core.h>

#include <string_view>
#include <vector>

namespace tenzir::tql2 {

TENZIR_ENUM(
  /// TODO
  token_kind,
  //
  identifier, dollar_ident,
  // keywords
  this_, if_, else_, match,
  // literals
  integer, real, true_, false_, null, string, ipv4, ipv6,
  // operators
  dot, plus, minus, slash, star, equal_equal, bang_equal, less, less_equal,
  greater, greater_equal,
  // other punctuation
  at, equal, comma, colon, single_quote,
  // parenthesis
  lpar, rpar, lbrace, rbrace, lbracket, rbracket,
  //
  pipe, newline,
  // trivia
  whitespace, delim_comment, line_comment,
  // special
  error);

inline auto describe(token_kind k) -> std::string_view {
  using enum token_kind;
#define X(x, y)                                                                \
  case x:                                                                      \
    return y
  switch (k) {
    X(identifier, "identifier");
    X(this_, "`this`");
    X(if_, "`if`");
    X(else_, "`else`");
    X(match, "`match`");
    X(integer, "integer");
    X(real, "real");
    X(true_, "`true`");
    X(false_, "`false`");
    X(null, "`null`");
    X(dot, "`.`");
    X(plus, "`+`");
    X(minus, "`-`");
    X(star, "`*`");
    X(slash, "`/`");
    X(equal_equal, "`==`");
    X(bang_equal, "`!=`");
    X(less, "`<`");
    X(less_equal, "`<=`");
    X(greater, "`>`");
    X(greater_equal, "`>=`");
    X(pipe, "`|`");
    X(lpar, "`(`");
    X(rpar, "`)`");
    X(lbrace, "`{`");
    X(rbrace, "`}`");
    X(lbracket, "`[`");
    X(rbracket, "`]`");
    // TODO
    default:
      return to_string(k);
  }
#undef X
  TENZIR_UNREACHABLE();
}

struct token {
  token(token_kind kind, size_t end) : kind{kind}, end{end} {
  }

  token_kind kind;
  size_t end;
};

auto lex(std::string_view content) -> std::vector<token>;

} // namespace tenzir::tql2

// template <>
// struct fmt::formatter<tenzir::tql2::token> {
//   constexpr auto parse(format_parse_context& ctx)
//     -> format_parse_context::iterator {
//     return ctx.begin();
//   }

//   auto format(const tenzir::tql2::token& x, format_context& ctx) const
//     -> format_context::iterator {
//     return fmt::format_to(ctx.out(), "{}:{}", to_string(x.kind), x.end);
//   }
// };
