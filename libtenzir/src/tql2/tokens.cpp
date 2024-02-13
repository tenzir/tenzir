//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/lexer.hpp>

namespace tenzir::tql2 {

auto describe(token_kind k) -> std::string_view {
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

} // namespace tenzir::tql2
