//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/default_formatter.hpp"

#include <tenzir/detail/enum.hpp>

#include <fmt/core.h>

#include <string_view>
#include <vector>

namespace tenzir::tql {

TENZIR_ENUM(
  /// TODO
  token_kind,
  // basics
  identifier,
  // ??
  meta, type_extractor,
  // literals
  integer, real, true_, false_, null, string,
  // operators
  pipe, logical_or, greater, dot, minus,
  // other punctuation
  assign, equals, comma,
  // parenthesis
  lpar, rpar,
  // newlines
  newline,
  // trivia
  whitespace, delim_comment, line_comment,
  // special
  error);

struct token {
  token_kind kind;
  size_t end;
};

auto lex(std::string_view content) -> std::vector<token>;

struct parse_tree {
  struct node {
    // TODO: Enum instead of string.
    std::string kind;
    size_t begin{};
    size_t end{};
    // 0 means nothing
    size_t first_child = 0;
    size_t right_sibling = 0;

    friend auto inspect(auto& f, node& x) -> bool {
      return f.object(x).fields(f.field("kind", x.kind),
                                f.field("begin", x.begin),
                                f.field("end", x.end),
                                f.field("first_child", x.first_child),
                                f.field("right_sibling", x.right_sibling));
    }
  };

  std::vector<node> nodes;
};

auto parse(std::span<token> tokens) -> parse_tree;

} // namespace tenzir::tql

template <>
struct tenzir::enable_default_formatter<tenzir::tql::parse_tree::node>
  : std::true_type {};

template <>
struct fmt::formatter<tenzir::tql::token> {
  constexpr auto parse(format_parse_context& ctx)
    -> format_parse_context::iterator {
    return ctx.begin();
  }

  auto format(const tenzir::tql::token& x, format_context& ctx) const
    -> format_context::iterator {
    return fmt::format_to(ctx.out(), "{}:{}", to_string(x.kind), x.end);
  }
};
