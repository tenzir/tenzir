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

#include <stack>
#include <string_view>

namespace tenzir {

auto tokenize(std::string_view content, session ctx)
  -> failure_or<std::vector<token>> {
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
  struct in_string {
    in_string(int64_t hashes, bool format, bool raw)
      : hashes{hashes}, format{format}, raw{raw} {
    }

    int64_t hashes;
    bool format;
    bool raw;
  };
  struct in_replacement {
    int64_t braces = 0;
  };
  auto stack = std::stack<variant<in_string, in_replacement>>{};
  auto string_info = [&]() -> in_string& {
    TENZIR_ASSERT(not stack.empty());
    return as<in_string>(stack.top());
  };
  auto is_format_string = [&] {
    return string_info().format;
  };
  auto is_non_raw_string = [&] {
    return not string_info().raw;
  };
  auto is_non_format_string = [&] {
    return not is_format_string();
  };
  auto string_hashes = [&] {
    return string_info().hashes;
  };
  // clang-format off
  auto normal_parser
    = ignore(ip >> "/" >> *digit)
      ->* [] { return tk::subnet; }
    | ignore(ip)
      ->* [] { return tk::ip; }
    | ignore(+digit >> '-' >> +digit >> '-' >> +digit
        >> *(alnum | ':' | '+' | '-') >> ~('.' >> +digit)
        >> ~('Z' | (('+' | '-') >> +digit)))
      ->* [] { return tk::datetime; }
    | ignore(digit >> *digit_us >> -('.' >> digit >> *digit_us) >> -identifier)
      ->* [] { return tk::scalar; }
    | ignore(ch<'\"'> )
      ->* [] { return tk::string_begin; }
    | ignore(ch<'r'> >> *ch<'#'> >> '"')
      ->* [] { return tk::raw_string_begin; }
    | ignore(ch<'b'> >> *ch<'#'> >> '"')
      ->* [] { return tk::blob_begin; }
    | ignore(lit{"br"} >> *ch<'#'> >> '"')
      ->* [] { return tk::raw_blob_begin; }
    | ignore(lit{"f"} >> '"')
      ->* [] { return tk::format_string_begin; }
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
    | X(".?", dot_question_mark)
    | X(".", dot)
    | X("?", question_mark)
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
    | X("not", not_)
    | X("null", null)
    | X("or", or_)
    | X("move", move)
    | X("this", this_)
    | X("true", true_)
#undef X
    | ignore((
        lit{"self"} | "is" | "as" | "use" /*| "type"*/ | "return" | "def" | "function"
        | "fn" | "super" | "for" | "while" | "mod" | "module"
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
      ->* [] { return tk::whitespace; };
  auto common_content
    // Quotes are allowed in strings with a '#' prefix.
    = lit{"\""} >> !function_repeat_parser{ch<'#'>, string_hashes}
    // They are also allowed in non-raw strings if preceded by backslash.
    | lit{"\\\""}.when(is_non_raw_string)
    // We also need to handle double backslashes to consume both at once.
    | lit{"\\\\"}.when(is_non_raw_string);
  auto closing_quote
    = ignore(lit{"\""} >> function_repeat_parser{ch<'#'>, string_hashes})
      ->* [] { return tk::closing_quote; };
  auto string_content
    = ignore(+(common_content | any - closing_quote))
      ->* [] { return tk::char_seq; }
    | closing_quote;
  auto format_string_content
    = ignore(+(common_content | "{{" | "}}" | any - closing_quote - '{' - '}'))
      ->* [] { return tk::char_seq; }
    | ignore(lit{"{"})
      ->* [] { return tk::fmt_begin; }
    | ignore(lit{"}"})
      ->* [] { return tk::fmt_end; }
    | closing_quote;
  auto string_parser
    = string_content.when(is_non_format_string)
    | format_string_content.when(is_format_string);
  // clang-format on
  auto current = content.begin();
  while (current != content.end()) {
    auto kind = tk{};
    auto success = false;
    if (stack.empty() or is<in_replacement>(stack.top())) {
      const auto start = current;
      success = normal_parser.parse(current, content.end(), kind);
      if (success) {
        auto normal_begin = kind == tk::string_begin or kind == tk::blob_begin;
        auto format_begin = kind == tk::format_string_begin;
        auto raw_begin
          = kind == tk::raw_string_begin or kind == tk::raw_blob_begin;
        if (normal_begin or format_begin or raw_begin) {
          stack.emplace(in_string{
            std::count(start, current, '#'),
            format_begin,
            raw_begin,
          });
        } else if (not stack.empty()) {
          auto& rep = as<in_replacement>(stack.top());
          if (kind == tk::lbrace) {
            rep.braces += 1;
          } else if (kind == tk::rbrace) {
            rep.braces -= 1;
            if (rep.braces < 0) {
              stack.pop();
              kind = tk::fmt_end;
            }
          }
        }
      }
    } else {
      success = string_parser.parse(current, content.end(), kind);
      if (success) {
        if (kind == tk::fmt_begin) {
          stack.emplace(in_replacement{});
        } else if (kind == tk::closing_quote) {
          TENZIR_ASSERT(not stack.empty());
          stack.pop();
        } else if (kind == tk::fmt_end) {
          // We ignore this here but catch it within the parser.
        } else {
          TENZIR_ASSERT(kind == tk::char_seq);
        }
      }
    }
    if (success) {
      result.emplace_back(kind, current - content.begin());
    } else {
      // We could not parse a token starting from `current`. Instead, we emit
      // a special `error` token and go to the next character.
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
    X(blob_begin, "`b\"`");
    X(char_seq, "character sequence");
    X(closing_quote, "`\"`");
    X(colon_colon, "`::`");
    X(colon, "`:`");
    X(comma, "`,`");
    X(datetime, "datetime");
    X(delim_comment, "`/*...*/`");
    X(dollar_ident, "dollar identifier");
    X(dot_dot_dot, "`...`");
    X(dot_question_mark, "`.?`");
    X(dot, "`.`");
    X(else_, "`else`");
    X(equal_equal, "`==`");
    X(equal, "`=`");
    X(error, "error");
    X(false_, "`false`");
    X(fat_arrow, "`=>`");
    X(fmt_begin, "`{`");
    X(fmt_end, "`}`");
    X(format_string_begin, "format string");
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
    X(minus, "`-`");
    X(move, "`move`");
    X(newline, "newline");
    X(not_, "`not`");
    X(null, "`null`");
    X(or_, "`or`");
    X(pipe, "`|`");
    X(plus, "`+`");
    X(question_mark, "`?`");
    X(raw_blob_begin, "`br\"`");
    X(raw_string_begin, "`r\"`");
    X(rbrace, "`}`");
    X(rbracket, "`]`");
    X(reserved_keyword, "reserved keyword");
    X(rpar, "`)`");
    X(scalar, "scalar");
    X(single_quote, "`'`");
    X(slash, "`/`");
    X(star, "`*`");
    X(string_begin, "`\"`");
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
  if (arrow::util::ValidateUTF8(content)) {
    return {};
  }
  // TODO: Consider reporting offset.
  diagnostic::error("found invalid UTF8").emit(ctx);
  return failure::promise();
}

} // namespace tenzir
