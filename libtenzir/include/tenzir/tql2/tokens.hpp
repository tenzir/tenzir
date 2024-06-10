//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/enum.hpp"

#include <string_view>

namespace tenzir {

TENZIR_ENUM(
  ///
  token_kind,
  // identifiers
  identifier, dollar_ident,
  // keywords
  this_, if_, else_, match, not_, and_, or_, underscore, let, in, meta,
  reserved_keyword,
  // literals
  scalar, true_, false_, null, string, ip, datetime,
  // punctuation
  dot, plus, minus, slash, star, equal_equal, bang_equal, less, less_equal,
  greater, greater_equal, at, equal, comma, colon, single_quote, fat_arrow,
  pipe,
  // parenthesis
  lpar, rpar, lbrace, rbrace, lbracket, rbracket,
  // whitespace
  newline, whitespace,
  // comments
  delim_comment, line_comment,
  // special
  error);

auto describe(token_kind k) -> std::string_view;

struct token {
  token(token_kind kind, size_t end) : kind{kind}, end{end} {
  }

  token_kind kind;
  size_t end;
};

auto tokenize(std::string_view content) -> std::vector<token>;

} // namespace tenzir
