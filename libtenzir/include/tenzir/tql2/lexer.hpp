//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/default_formatter.hpp>
#include <tenzir/detail/enum.hpp>

#include <fmt/core.h>

#include <string_view>
#include <vector>

namespace tenzir::tql2 {

TENZIR_ENUM(
  /// TODO
  token_kind,
  // keywords,
  this_, // TODO
  // basics
  identifier,
  // ??
  meta, type_extractor,
  // literals
  integer, real, true_, false_, null, string, ipv4, ipv6,
  // operators
  pipe, logical_or, greater, dot, minus, cmp_equals,
  // other punctuation
  equal, comma, colon, quote,
  // parenthesis
  lpar, rpar, lbrace, rbrace,
  // newline
  newline,
  // trivia
  whitespace, delim_comment, line_comment,
  // special
  error);

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
