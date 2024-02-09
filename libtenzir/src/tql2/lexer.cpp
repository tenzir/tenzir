//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/core/end_of_input.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/string/literal.hpp>
#include <tenzir/tql2/lexer.hpp>

namespace tenzir::tql2 {

auto lex(std::string_view content) -> std::vector<token> {
  std::vector<token> result;
  const auto* current = content.begin();
  // clang-format off
  using namespace parsers;
  auto identifier = (alpha | chr{'_'}) >> *(alnum | chr{'_'});
  auto p
    = ignore(*xdigit >> chr{':'} >> *xdigit >> chr{':'} >> *xdigit >> *((chr{'.'} | chr{':'}) >> *xdigit))
      ->* [] { return token_kind::ipv6; }
    | ignore(+digit >> chr{'.'} >> +digit >> +(chr{'.'} >> +digit))
      ->* [] { return token_kind::ipv4; }
    | ignore(+digit >> chr{'.'} >> +digit)
      ->* [] { return token_kind::real; }
    | ignore(+digit)
      ->* [] { return token_kind::integer; }
    | ignore(lit{"this"} >> !&(parsers::alnum | '_'))
      ->* [] { return token_kind::this_; }
    | ignore(chr{'"'} >> *(any - chr{'"'}) >> (chr{'"'} | eoi))
      ->* [] { return token_kind::string; }
    | ignore((lit{"//"} | lit{"# "}) >> *(any - '\n'))
      ->* [] { return token_kind::line_comment; }
    | ignore(lit{"/*"} >> *(any - lit{"*/"}) >> (lit{"*/"} | eoi))
      ->* [] { return token_kind::delim_comment; }
    | ignore(chr{'#'} >> identifier)
      ->* [] { return token_kind::meta; }
#define X(x, y) ignore(lit{x}) ->* [] { return token_kind::y; }
    | X("||", logical_or)
    | X("|", pipe)
    | X(">", greater)
    | X(".", dot)
    | X("==", cmp_equals)
    | X("=", equal)
    | X("(", lpar)
    | X(")", rpar)
    | X("{", lbrace)
    | X("}", rbrace)
    | X("-", minus)
    | X(",", comma)
    | X(":", colon)
    | X("'", quote)
    | X("\n", newline)
#undef X
    | ignore(identifier)
      ->* [] { return token_kind::identifier; }
    | ignore(+(space - '\n'))
      ->* [] { return token_kind::whitespace; }
  ;
  // clang-format on
  while (current != content.end()) {
    auto kind = token_kind{};
    if (p.parse(current, content.end(), kind)) {
      result.emplace_back(kind, current - content.begin());
    } else {
      ++current;
      auto end = current - content.begin();
      if (result.empty() || result.back().kind != token_kind::error) {
        result.emplace_back(token_kind::error, end);
      } else {
        result.back().end = end;
      }
    }
  }
  return result;
}

} // namespace tenzir::tql2
