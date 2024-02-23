//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/tokens.hpp"

namespace tenzir::tql2 {

auto describe(token_kind k) -> std::string_view {
  using enum token_kind;
#define X(x, y)                                                                \
  case x:                                                                      \
    return y
  switch (k) {
    X(and_, "`and`");
    X(at, "@");
    X(bang_equal, "`!=`");
    X(colon, "`:`");
    X(comma, "`,`");
    X(datetime, "datetime");
    X(delim_comment, "`/*...*/`");
    X(dollar_ident, "dollar identifier");
    X(dot, "`.`");
    X(else_, "`else`");
    X(equal_equal, "`==`");
    X(equal, "`=`");
    X(error, "error");
    X(false_, "`false`");
    X(fat_arrow, "`=>`");
    X(greater_equal, "`>=`");
    X(greater, "`>`");
    X(identifier, "identifier");
    X(if_, "`if`");
    X(ipv4, "ipv4");
    X(ipv6, "ipv6");
    X(lbrace, "`{`");
    X(lbracket, "`[`");
    X(less_equal, "`<=`");
    X(less, "`<`");
    X(let, "`let`");
    X(line_comment, "`// ...`");
    X(lpar, "`(`");
    X(match, "`match`");
    X(minus, "`-`");
    X(newline, "newline");
    X(not_, "`not`");
    X(null, "`null`");
    X(number, "number");
    X(or_, "`or`");
    X(pipe, "`|`");
    X(plus, "`+`");
    X(rbrace, "`}`");
    X(rbracket, "`]`");
    X(rpar, "`)`");
    X(single_quote, "`'`");
    X(slash, "`/`");
    X(star, "`*`");
    X(string, "string");
    X(this_, "`this`");
    X(true_, "`true`");
    X(underscore, "`_`");
    X(whitespace, "whitespace");
  }
#undef X
  TENZIR_UNREACHABLE();
}

} // namespace tenzir::tql2
