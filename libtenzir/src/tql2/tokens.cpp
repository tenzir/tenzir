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

auto tokenize(std::string_view content,
              session ctx) -> failure_or<std::vector<token>> {
  TRY(validate_utf8(content, ctx));
  auto tokens = tokenize_permissive(content);
  TRY(verify_tokens(tokens, ctx));
  return tokens;
}

auto tokenize_permissive(std::string_view content) -> std::vector<token> {
  using namespace parsers;
  using tk = token_kind;
  auto result = std::vector<token>{};
  // TODO: The char-class parsers (such as `parsers::alnum`) can cause undefined
  // behavior. We should fix them or use something different here.
  auto continue_ident = alnum | '_';
  auto identifier = (alpha | '_') >> *continue_ident;
  auto digit_us = digit | '_';
  // Note that many parsers here are not strict, but very lenient instead. This
  // is so that we can tokenize even if the input is malformed, which produces
  // better error messages.
  auto ipv4 = +digit >> '.' >> +digit >> +('.' >> +digit);
  auto ipv6 = *xdigit >> ':' >> *xdigit >> ':' >> *xdigit
              >> *(('.' >> +digit) | (':' >> *xdigit));
  auto ipv6_enabled = true;
  auto ip = ipv4 | ipv6.when([&] {
    return ipv6_enabled;
  });
  // clang-format off
  auto p
    = ignore(ip >> "/" >> *digit)
      ->* [] { return tk::subnet; }
    | ignore(ip)
      ->* [] { return tk::ip; }
    | ignore(+digit >> '-' >> +digit >> '-' >> +digit >> *(alnum | ':' | '+' | '-'))
      ->* [] { return tk::datetime; }
    | ignore(digit >> *digit_us >> -('.' >> digit >> *digit_us) >> -identifier)
      ->* [] { return tk::scalar; }
    | ignore('"' >> *(('\\' >> any) | (any - '"')) >> '"')
      ->* [] { return tk::string; }
    | ignore('"' >> *(('\\' >> any) | (any - '"')))
      ->* [] { return tk::error; } // non-terminated string
    | ignore("r\"" >> *(any - '"') >> '"')
      ->* [] { return tk::raw_string; }
    | ignore("r\"" >> *(any - '"'))
      ->* [] { return tk::error; } // non-terminated raw string
    | ignore("r#\"" >> *(any - "\"#") >> "\"#")
      ->* [] { return tk::raw_string; }
    | ignore("r#\"" >> *(any - "\"#"))
      ->* [] { return tk::error; } // non-terminated raw string
    | ignore("//" >> *(any - '\n'))
      ->* [] { return tk::line_comment; }
    | ignore("/*" >> *(any - "*/") >> "*/")
      ->* [] { return tk::delim_comment; }
    | ignore("/*" >> *(any - "*/"))
      ->* [] { return tk::error; } // non-terminated comment
    | ignore(ch<'@'>)
      ->* [] { return tk::at; }
#define X(x, y) ignore(lit{x}) ->* [] { return tk::y; }
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
    // The double colon is shadowed by IPv6 unless that is disabled.
    | X("::", colon_colon)
    | X(":", colon)
    | X("'", single_quote)
    | X("\n", newline)
#undef X
#define X(x, y) ignore(lit{x} >> !continue_ident) ->* [] { return tk::y; }
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
      ) >> !continue_ident) ->* [] { return tk::reserved_keyword; }
    | ignore('$' >> identifier)
      ->* [] { return tk::dollar_ident; }
    | ignore('_' >> !continue_ident)
      ->* [] { return tk::underscore; }
    | ignore(identifier)
      ->* [] { return tk::identifier; }
    | ignore(
        +((space - '\n') |
        ("\\" >> *(space - '\n') >> '\n')) |
        ("#!" >> *(any - '\n')).when([&] { return result.empty(); })
      )
      ->* [] { return tk::whitespace; }
  ;
  // clang-format on
  auto current = content.begin();
  while (current != content.end()) {
    // // This is a workaround in order to make `::` in `pkg::add` tokenize as
    // // `pkg`, `::` (double colon), `add`, instead of `pkg`, `::add` (IPv6).
    // TENZIR_WARN("rest: {:?}", std::string_view{current, content.end()});
    // if (std::string_view{current, content.end()}.starts_with("::")
    //     and not result.empty()
    //     and result.back().kind == token_kind::identifier) {
    //   current += 2;
    //   result.emplace_back(token_kind::colon_colon, current -
    //   content.begin()); continue;
    // }
    auto kind = tk{};
    if (p.parse(current, content.end(), kind)) {
      result.emplace_back(kind, current - content.begin());
    } else {
      // We could not parse a token starting from `current`. Instead, we emit a
      // special `error` token and go to the next character.
      ++current;
      auto end = current - content.begin();
      if (result.empty() || result.back().kind != tk::error) {
        result.emplace_back(tk::error, end);
      } else {
        // If the last token is already an error, we just expand it instead.
        result.back().end = end;
      }
      kind = tk::error;
    }
    // Disable IPv6 in the next iteration of we previously read `ident` or `::`.
    ipv6_enabled = kind != tk::identifier and kind != tk::colon_colon;
  }
  return result;
}

auto verify_tokens(std::span<const token> tokens,
                   session ctx) -> failure_or<void> {
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
    X(colon_colon, "`::`");
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
    X(raw_string, "raw string");
    X(rbrace, "`}`");
    X(rbracket, "`]`");
    X(reserved_keyword, "reserved keyword");
    X(rpar, "`)`");
    X(scalar, "scalar");
    X(single_quote, "`'`");
    X(slash, "`/`");
    X(star, "`*`");
    X(string, "string");
    X(subnet, "subnet");
    X(this_, "`this`");
    X(true_, "`true`");
    X(underscore, "`_`");
    X(whitespace, "whitespace");
  }
#undef X
  TENZIR_UNREACHABLE();
}

auto validate_utf8(std::string_view content, session ctx) -> failure_or<void> {
  // TODO: Refactor this.
  arrow::util::InitializeUTF8();
  if (arrow::util::ValidateUTF8(content)) {
    return {};
  }
  // TODO: Consider reporting offset.
  diagnostic::error("found invalid UTF8").emit(ctx);
  return failure::promise();
}

} // namespace tenzir
