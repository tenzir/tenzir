//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/tokens.hpp"

#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/session.hpp"
#include "tenzir/try.hpp"

#include <arrow/util/utf8.h>

namespace tenzir {

auto tokenize(std::string_view content, session ctx)
  -> failure_or<std::vector<token>> {
  auto tokens = tokenize_permissive(content);
  TRY(verify_tokens(tokens, ctx));
  return tokens;
}

auto tokenize_permissive(std::string_view content) -> std::vector<token> {
  auto result = std::vector<token>{};
  // TODO: The char-class parsers (such as `parsers::alnum`) can cause undefined
  // behavior. We should fix them or use something different here.
  using namespace parsers;
  // clang-format off
  auto continue_ident = alnum | '_';
  auto identifier = (alpha | '_') >> *continue_ident;
  auto digit_us = digit | '_';
  auto p
    = ignore(*xdigit >> ':' >> *xdigit >> ':' >> *xdigit
             >> *(('.' >> +digit) | (':' >> *xdigit)))
      ->* [] { return token_kind::ip; }
    | ignore(+digit >> '.' >> +digit >> +('.' >> +digit))
      ->* [] { return token_kind::ip; }
    | ignore(+digit >> '-' >> +digit >> '-' >> +digit >> *(alnum | ':' | '+' | '-'))
      ->* [] { return token_kind::datetime; }
    | ignore(digit >> *digit_us >> -('.' >> digit >> *digit_us) >> -identifier)
      ->* [] { return token_kind::scalar; }
    | ignore('"' >> *(('\\' >> any) | (any - '"')) >> '"')
      ->* [] { return token_kind::string; }
    | ignore('"' >> *(('\\' >> any) | (any - '"')))
      ->* [] { return token_kind::error; } // non-terminated string
    | ignore("//" >> *(any - '\n'))
      ->* [] { return token_kind::line_comment; }
    | ignore("/*" >> *(any - "*/") >> "*/")
      ->* [] { return token_kind::delim_comment; }
    | ignore("/*" >> *(any - "*/"))
      ->* [] { return token_kind::error; } // non-terminated comment
    | ignore(ch<'@'>)
      ->* [] { return token_kind::at; }
#define X(x, y) ignore(lit{x}) ->* [] { return token_kind::y; }
    | X("=>", fat_arrow)
    | X("==", equal_equal)
    | X("!=", bang_equal)
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
    | X("...", dot_dot_dot)
    | X(".", dot)
    | X("(", lpar)
    | X(")", rpar)
    | X("{", lbrace)
    | X("}", rbrace)
    | X("[", lbracket)
    | X("]", rbracket)
    | X(",", comma)
    | X(":", colon)
    | X("'", single_quote)
    | X("\n", newline)
#undef X
#define X(x, y) ignore(lit{x} >> !continue_ident) ->* [] { return token_kind::y; }
    | X("and", and_)
    | X("else", else_)
    | X("false", false_)
    | X("if", if_)
    | X("in", in)
    | X("let", let)
    | X("match", match)
    | X("meta", meta)
    | X("not", not_)
    | X("null", null)
    | X("or", or_)
    | X("this", this_)
    | X("true", true_)
#undef X
    | ignore((
        lit{"self"} | "is" | "as" | "use" /*| "type"*/ | "return" | "def" | "function"
        | "fn" | "pipeline" | "meta" | "super" | "for" | "while" | "mod" | "module"
      ) >> !continue_ident) ->* [] { return token_kind::reserved_keyword; }
    | ignore('$' >> identifier)
      ->* [] { return token_kind::dollar_ident; }
    | ignore('_' >> !continue_ident)
      ->* [] { return token_kind::underscore; }
    | ignore(identifier)
      ->* [] { return token_kind::identifier; }
    | ignore(
        +((space - '\n') |
        ("\\" >> *(space - '\n') >> '\n')) |
        ("#!" >> *(any - '\n')).when([&] { return result.empty(); })
      )
      ->* [] { return token_kind::whitespace; }
  ;
  // clang-format on
  auto current = content.begin();
  while (current != content.end()) {
    auto kind = token_kind{};
    if (p.parse(current, content.end(), kind)) {
      result.emplace_back(kind, current - content.begin());
    } else {
      // We could not parse a token starting from `current`. Instead, we emit a
      // special `error` token and go to the next character.
      ++current;
      auto end = current - content.begin();
      if (result.empty() || result.back().kind != token_kind::error) {
        result.emplace_back(token_kind::error, end);
      } else {
        // If the last token is already an error, we just expand it instead.
        result.back().end = end;
      }
    }
  }
  return result;
}

auto verify_tokens(std::span<const token> tokens, session ctx)
  -> failure_or<void> {
  auto result = failure_or<void>{};
  for (auto& token : tokens) {
    if (token.kind == token_kind::error) {
      auto begin = size_t{0};
      if (&token != tokens.data()) {
        begin = (&token - 1)->end;
      }
      diagnostic::error("could not parse token")
        .primary(location{begin, token.end})
        .emit(ctx);
      result = failure::promise();
    }
  }
  return result;
}

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
    X(dot_dot_dot, "`...`");
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
    X(in, "`in`");
    X(ip, "ip");
    X(lbrace, "`{`");
    X(lbracket, "`[`");
    X(less_equal, "`<=`");
    X(less, "`<`");
    X(let, "`let`");
    X(line_comment, "`// ...`");
    X(lpar, "`(`");
    X(match, "`match`");
    X(meta, "`meta`");
    X(minus, "`-`");
    X(newline, "newline");
    X(not_, "`not`");
    X(null, "`null`");
    X(or_, "`or`");
    X(pipe, "`|`");
    X(plus, "`+`");
    X(rbrace, "`}`");
    X(rbracket, "`]`");
    X(reserved_keyword, "reserved keyword");
    X(rpar, "`)`");
    X(scalar, "scalar");
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

} // namespace tenzir
