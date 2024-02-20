//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/lexer.hpp"

#include "tenzir/concept/parseable/string/char_class.hpp"

#include <arrow/util/utf8.h>

namespace tenzir::tql2 {

auto lex(std::string_view content) -> std::vector<token> {
  std::vector<token> result;
  const auto* current = content.begin();
  // clang-format off
  using namespace parsers;
  auto continue_ident = alnum | chr{'_'};
  auto identifier = (alpha | chr{'_'}) >> *continue_ident;
  auto p
    = ignore(*xdigit >> chr{':'} >> *xdigit >> chr{':'} >> *xdigit >> *((chr{'.'} | chr{':'}) >> *xdigit))
      ->* [] { return token_kind::ipv6; }
    | ignore(+digit >> chr{'.'} >> +digit >> +(chr{'.'} >> +digit))
      ->* [] { return token_kind::ipv4; }
    | ignore(+digit >> chr{'.'} >> +digit)
      ->* [] { return token_kind::real; }
    | ignore(+digit)
      ->* [] { return token_kind::integer; }
    | ignore(chr{'"'} >> *(lit{"\\\""} | (any - chr{'"'})) >> chr{'"'})
      ->* [] { return token_kind::string; }
    | ignore(chr{'"'} >> *(lit{"\\\""} | (any - chr{'"'})))
      ->* [] { return token_kind::error; } // non-terminated string
    | ignore((lit{"//"} | lit{"# "}) >> *(any - '\n'))
      ->* [] { return token_kind::line_comment; }
    | ignore(lit{"/*"} >> *(any - lit{"*/"}) >> lit{"*/"})
      ->* [] { return token_kind::delim_comment; }
    | ignore(lit{"/*"} >> *(any - lit{"*/"}))
      ->* [] { return token_kind::error; } // non-terminated comment
    | ignore(chr{'@'})
      ->* [] { return token_kind::at; }
#define X(x, y) ignore(lit{x}) ->* [] { return token_kind::y; }
    | X("=>", fat_arrow)
    | X("==", equal_equal)
    | X("!=", equal_equal)
    | X(">=", greater_equal)
    | X("<=", less_equal)
    | X(">", greater)
    | X("<", less)
    | X("+", plus)
    | X("-", minus)
    | X("*", star)
    | X("/", slash)
    | X("=", equal)
    | X("|", pipe)
    | X(".", dot)
    | X("(", lpar)
    | X(")", rpar)
    | X("{", lbrace)
    | X("}", rbrace)
    | X(",", comma)
    | X(":", colon)
    | X("_", underscore)
    | X("'", single_quote)
    | X("\n", newline)
#undef X
#define X(x, y) ignore(lit{x} >> !continue_ident) ->* [] { return token_kind::y; }
    | X("this", this_)
    | X("let", let)
    | X("if", if_)
    | X("else", else_)
    | X("match", match)
    | X("and", and_)
    | X("or", or_)
    | X("not", not_)
    | X("true", true_)
    | X("false", false_)
    | X("null", null)
#undef X
    | ignore(chr{'$'} >> identifier)
      ->* [] { return token_kind::dollar_ident; }
    | ignore(identifier)
      ->* [] { return token_kind::identifier; }
    | ignore(+((space - '\n') | ("\\" >> *(space - '\n') >> '\n')))
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
