//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/detail/enum.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"

#include <string_view>

namespace tenzir {

TENZIR_ENUM(
  ///
  token_kind,
  // identifiers
  identifier, dollar_ident,
  // keywords
  this_, if_, else_, match, not_, and_, or_, move, underscore, let, in,
  reserved_keyword,
  // literals
  scalar, true_, false_, null, ip, subnet, datetime,
  // strings
  string_begin, raw_string_begin, blob_begin, raw_blob_begin,
  format_string_begin, char_seq, fmt_begin, fmt_end, closing_quote,
  // punctuation
  dot, dot_question_mark, question_mark, plus, minus, slash, star, equal_equal,
  bang_equal, less, less_equal, greater, greater_equal, at, equal, comma, colon,
  single_quote, fat_arrow, pipe, dot_dot_dot, colon_colon,
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
  struct parsers {
    static constexpr const auto continue_ident = tenzir::parsers::alnum | '_';
    static constexpr const auto identifier
      = (tenzir::parsers::alpha | '_') >> *continue_ident;
  };

  token(token_kind kind, size_t end) : kind{kind}, end{end} {
  }

  token_kind kind;
  size_t end;
};

/// Try to tokenize the source. This is a combination of calling:
/// - validate_utf8
/// - tokenize_permissive
/// - verify_tokens
auto tokenize(std::string_view content, session ctx)
  -> failure_or<std::vector<token>>;

/// Checks that the source is valid UTF-8.
auto validate_utf8(std::string_view content, session ctx) -> failure_or<void>;

/// Tokenize without emitting errors for error tokens.
auto tokenize_permissive(std::string_view content) -> std::vector<token>;

/// Emit errors for error tokens.
auto verify_tokens(std::span<token const> tokens, session ctx)
  -> failure_or<void>;

} // namespace tenzir
