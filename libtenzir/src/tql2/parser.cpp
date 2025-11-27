//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/parser.hpp"

#include "tenzir/checked_math.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/subnet.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/eval.hpp"

#include <arrow/util/utf8.h>

#include <ranges>
#include <string_view>

namespace tenzir {

namespace {

using namespace ast;

auto precedence(unary_op x) -> int {
  using enum unary_op;
  switch (x) {
    case move:
      return 10;
    case pos:
    case neg:
      return 9;
    case not_:
      return 5;
  };
  TENZIR_UNREACHABLE();
}

auto precedence(binary_op x) -> int {
  using enum binary_op;
  switch (x) {
    case mul:
    case div:
      return 8;
    case add:
    case sub:
      return 7;
    case gt:
    case geq:
    case lt:
    case leq:
    case eq:
    case neq:
    case in:
      return 6;
    case and_:
      return 4;
    case or_:
      return 3;
    case if_:
      return 2;
    case else_:
      return 1;
  }
  TENZIR_UNREACHABLE();
}

class parser {
public:
  using tk = token_kind;

  template <class F>
  static auto parse_with(std::span<token> tokens, std::string_view source,
                         diagnostic_handler& diag, bool anonymous, F&& f)
    -> failure_or<std::invoke_result_t<F, parser&>> {
    try {
      auto self = parser{tokens, source, diag, anonymous};
      auto result = std::invoke(std::forward<F>(f), self);
      if (self.next_ != self.tokens_.size()) {
        self.throw_token("expected EOF");
      }
      return result;
    } catch (diagnostic& d) {
      TENZIR_ASSERT(d.severity == severity::error);
      diag.emit(std::move(d));
      return failure::promise();
    } catch (failure& f) {
      return f;
    }
  }

  struct accept_result {
    accept_result() = default;

    accept_result(std::string_view text, tenzir::location location)
      : text{text}, location{location} {
    }

    std::string_view text;
    tenzir::location location;

    explicit(false) operator located<std::string_view>() {
      TENZIR_ASSERT(*this);
      return {text, location};
    }

    explicit operator bool() const {
      // TODO: Is this okay?
      return text.data() != nullptr;
    }

    auto as_identifier() const -> ast::identifier {
      TENZIR_ASSERT(*this);
      return ast::identifier{text, location};
    }

    auto get_location() const -> tenzir::location {
      TENZIR_ASSERT(*this);
      return location;
    }
  };

  auto parse_pipeline() -> ast::pipeline {
    auto scope = ignore_newlines(false);
    auto body = std::vector<statement>{};
    while (true) {
      while (accept(tk::newline) || accept(tk::pipe)) {
      }
      if (at_pipeline_end()) {
        break;
      }
      body.push_back(parse_statement());
      if (not at_statement_end()) {
        throw_token();
      }
    }
    return ast::pipeline{std::move(body)};
  }

  auto parse_statement() -> statement {
    if (peek(tk::let)) {
      return parse_let_stmt();
    }
    if (peek(tk::if_)) {
      return parse_if_stmt();
    }
    if (peek(tk::match)) {
      return parse_match_stmt();
    }
    return parse_invocation_or_assignment();
  }

  auto parse_let_stmt() -> let_stmt {
    auto let = expect(tk::let);
    if (silent_peek(tk::identifier)) {
      auto ident = expect(tk::identifier);
      diagnostic::error("identifier after `let` must start with `$`")
        .primary(ident, "try `${}` instead", ident.text)
        .throw_();
    }
    auto name = expect(tk::dollar_ident);
    expect(tk::equal);
    auto expr = parse_expression();
    return let_stmt{
      let.location,
      name.as_identifier(),
      std::move(expr),
    };
  }

  auto parse_if_stmt() -> if_stmt {
    auto if_kw = expect(tk::if_);
    auto condition = parse_expression();
    expect(tk::lbrace);
    auto consequence = parse_pipeline();
    expect(tk::rbrace);
    auto alternative = std::optional<ast::if_stmt::else_t>{};
    // We allow newlines before `else`. However, if there is no `else`, then we
    // don't want to consume them.
    auto stash = next_;
    consume_trivia_with_newlines();
    if (auto else_kw = accept(tk::else_)) {
      alternative.emplace(else_kw.location, ast::pipeline{});
      if (peek(tk::if_)) {
        auto body = std::vector<statement>{};
        body.emplace_back(parse_if_stmt());
        alternative->pipe = ast::pipeline{std::move(body)};
      } else {
        expect(tk::lbrace);
        alternative->pipe = parse_pipeline();
        expect(tk::rbrace);
      }
    } else {
      next_ = stash;
    }
    return if_stmt{
      if_kw.location,
      std::move(condition),
      std::move(consequence),
      std::move(alternative),
    };
  }

  auto parse_match_stmt() -> match_stmt {
    // TODO: Decide exact syntax, useable for both single-line and
    // multi-line TQL, and for expressions and statements.
    //    match foo {
    //      "ok", 42 => { ... }
    //    }
    auto begin = expect(tk::match);
    auto expr = parse_expression();
    auto arms = std::vector<match_stmt::arm>{};
    expect(tk::lbrace);
    auto scope = ignore_newlines(true);
    // TODO: Restrict this.
    while (not peek(tk::rbrace)) {
      arms.push_back(parse_match_stmt_arm());
      // TODO: require comma or newline?
      (void)accept(tk::comma);
    }
    auto end = expect(tk::rbrace);
    return match_stmt{
      begin.location,
      std::move(expr),
      std::move(arms),
      end.location,
    };
  }

  auto parse_match_stmt_arm() -> match_stmt::arm {
    auto filter = std::vector<ast::expression>{};
    while (true) {
      filter.push_back(parse_expression(1));
      if (accept(tk::fat_arrow)) {
        break;
      }
      expect(tk::comma);
    }
    expect(tk::lbrace);
    auto pipe = parse_pipeline();
    expect(tk::rbrace);
    return match_stmt::arm{
      std::move(filter),
      std::move(pipe),
    };
  }

  auto parse_invocation_or_assignment() -> statement {
    // TODO: Proper entity parsing.
    if (silent_peek(tk::identifier) and silent_peek_n(tk::colon_colon, 1)) {
      auto entity = parse_entity();
      return parse_invocation(std::move(entity));
    }
    // This is a hack: `move` is both a unary expression operator and a
    // pipeline operator. In order to support both, we need to special-case the
    // pipeline operator parsing, as the tokenizer returns `tk::move` instead of
    // `tk::identifier`.
    if (silent_peek(tk::move)) {
      auto entity = parse_entity(expect(tk::move).as_identifier());
      return parse_invocation(std::move(entity));
    }
    auto unary_expr = parse_unary_expression();
    if (auto* call = try_as<ast::function_call>(unary_expr)) {
      // TODO: We patch a top-level function call to be an operator invocation
      // instead. This could be done differently by slightly rewriting the
      // parser. Because this is not (yet) reflected in the AST, the optional
      // parenthesis are not reflected.
      if (call->method) {
        // TODO: We could consider rewriting method calls to mutate their
        // subject, e.g., `foo.bar.baz(qux) => foo.bar = foo.bar.baz(qux)`.
        diagnostic::error("expected operator invocation, found method call")
          .primary(call->fn)
          .throw_();
      }
      if (not at_statement_end()) {
        if (call->args.size() == 1) {
          // This occurs in cases such as `where (x or y) and z`. We have now
          // parsed `where (x or y)` as a function call and have to rewrite.
          auto first_arg = parse_expression(std::move(call->args[0]));
          auto args = std::vector<ast::expression>{};
          args.push_back(std::move(first_arg));
          return parse_invocation(std::move(call->fn), std::move(args));
        }
        diagnostic::error("expected end of statement")
          .primary(next_location())
          .throw_();
      }
      return ast::invocation{std::move(call->fn), std::move(call->args)};
    }
    if (auto* type_expr = try_as<ast::type_expr>(unary_expr)) {
      auto* ident = try_as<ast::type_name>(type_expr->def);
      if (not ident) {
        diagnostic::error("expected identifier").primary(*type_expr).throw_();
      }
      auto equal = expect(tk::equal).location;
      auto def = parse_type_def();
      return ast::type_stmt{
        type_expr->keyword,
        ast::type_name{std::move(*ident)},
        equal,
        std::move(def),
      };
    }
    auto left = to_selector(std::move(unary_expr));
    if (auto equal = accept(tk::equal)) {
      auto right = parse_expression();
      return assignment{
        std::move(left),
        equal.location,
        std::move(right),
      };
    }
    // TODO: Improve this.
    auto simple_sel = std::get_if<field_path>(&left);
    auto root = simple_sel
                  ? std::get_if<ast::root_field>(&*simple_sel->inner().kind)
                  : nullptr;
    if (root) {
      auto entity = parse_entity(std::move(root->id));
      if (entity.path.size() == 1 and entity.path[0].name == "type") {
        diagnostic::error("expected identifier after `type` declaration")
          .primary(entity)
          .throw_();
      }
      return parse_invocation(std::move(entity));
    }
    diagnostic::error("{}", "expected `=` after selector")
      .primary(next_location())
      .throw_();
  }

  auto parse_type_def() -> ast::type_def {
    if (auto id = accept(tk::identifier)) {
      return type_name{id.as_identifier()};
    }
    if (auto lbracket = accept(tk::lbracket)) {
      auto scope = ignore_newlines(true);
      auto type = parse_type_def();
      auto rbracket = expect(tk::rbracket);
      return ast::list_def{
        lbracket.location,
        std::move(type),
        rbracket.location,
      };
    }
    auto start = expect(tk::lbrace).location;
    auto scope = ignore_newlines(true);
    auto fields = std::vector<ast::record_def::field>{};
    while (not peek(tk::rbrace)) {
      auto name = expect(tk::identifier).as_identifier();
      expect(tk::colon);
      auto def = parse_type_def();
      fields.emplace_back(std::move(name), std::move(def));
      if (not peek(tk::rbrace)) {
        expect(tk::comma);
      }
    }
    auto end = expect(tk::rbrace);
    return ast::record_def{
      start,
      std::move(fields),
      end.location,
    };
  }

  auto parse_invocation(entity op, std::vector<ast::expression> args)
    -> ast::invocation {
    while (not at_statement_end()) {
      if (not args.empty()) {
        if (not accept(tk::comma)) {
          // Allow `{ ... }` without comma as final argument.
          if (not peek(tk::lbrace)) {
            diagnostic::error("unexpected continuation of argument")
              .primary(next_location())
              .hint("try inserting a `,` before if this is the next argument")
              .throw_();
          }
          args.emplace_back(parse_record_or_pipeline_expr());
          if (at_statement_end()) {
            break;
          }
          auto before = args.back().match([](auto& x) {
            return x.get_location();
          });
          diagnostic::error("expected end of statement due to final argument")
            .primary(next_location(), "expected end of statement")
            .secondary(before, "final argument")
            .hint("insert a `,` before `{` to continue arguments")
            .throw_();
        }
        consume_trivia_with_newlines();
      }
      args.push_back(parse_expression());
    }
    return ast::invocation{
      std::move(op),
      std::move(args),
    };
  }

  auto parse_invocation(entity op) -> ast::invocation {
    return parse_invocation(std::move(op), {});
  }

  auto to_selector(ast::expression expr) -> selector {
    auto location = expr.get_location();
    auto result = selector::try_from(std::move(expr));
    if (not result) {
      // TODO: Improve error message.
      diagnostic::error("expected selector").primary(location).throw_();
    }
    return std::move(*result);
  }

  auto parse_assignment() -> ast::assignment {
    auto expr = parse_expression();
    return expr.match(
      [](ast::assignment& x) {
        return std::move(x);
      },
      [&](auto&) -> ast::assignment {
        diagnostic::error("expected assignment")
          .primary(expr.get_location())
          .throw_();
      });
  }

  auto parse_expression(ast::expression expr, int min_prec = 0)
    -> ast::expression {
    while (true) {
      if (min_prec == 0) {
        if (auto equal = accept(tk::equal)) {
          auto left = to_selector(expr);
          // TODO: Check precedence.
          auto right = parse_expression();
          expr = assignment{
            std::move(left),
            equal.location,
            std::move(right),
          };
          continue;
        }
        if (auto arrow = accept(tk::fat_arrow)) {
          auto* left = try_as<ast::root_field>(expr);
          if (not left or left->has_question_mark) {
            diagnostic::error("expected identifier").primary(expr).throw_();
          }
          auto right = parse_expression();
          expr = lambda_expr{std::move(left->id), arrow.location,
                             std::move(right)};
          continue;
        }
      }
      auto negate = std::optional<location>{};
      if (silent_peek(tk::not_) and silent_peek_n(tk::in, 1)
          and precedence(binary_op::in) >= min_prec) {
        negate = advance();
      }
      if (auto bin_op = peek_binary_op()) {
        auto new_prec = precedence(*bin_op);
        if (new_prec >= min_prec) {
          auto location = advance();
          consume_trivia_with_newlines();
          auto right = parse_expression(new_prec + 1);
          expr = binary_expr{
            std::move(expr),
            located{*bin_op, location},
            std::move(right),
          };
          if (negate) {
            expr = unary_expr{{unary_op::not_, *negate}, std::move(expr)};
          }
          continue;
        }
      }
      break;
    }
    // We clear the previously tried token to improve error messages.
    tries_.clear();
    return expr;
  }

  auto parse_expression(int min_prec = 0) -> ast::expression {
    return parse_expression(parse_unary_expression(), min_prec);
  }

  // TODO: Future us/ast problem with type expressions

  auto parse_entity() -> ast::entity {
    return parse_entity(expect(tk::identifier).as_identifier());
  }

  auto parse_entity(ast::identifier root) -> ast::entity {
    auto path = std::vector<identifier>{};
    path.push_back(std::move(root));
    while (accept(tk::colon_colon)) {
      path.push_back(expect(tk::identifier).as_identifier());
    }
    return ast::entity{std::move(path)};
  }

  auto parse_unary_expression() -> ast::expression {
    if (auto op = peek_unary_op()) {
      auto location = advance();
      auto expr = parse_expression(precedence(*op));
      return unary_expr{
        located{*op, location},
        std::move(expr),
      };
    }
    auto expr = parse_primary_expression();
    while (true) {
      auto dot = accept(tk::dot_question_mark);
      auto has_question_mark = static_cast<bool>(dot);
      if (has_question_mark) {
        diagnostic::warning(
          "leading `.?` is deprecated; use a trailing `?` instead")
          .primary(dot)
          .emit(diag_);
      } else {
        dot = accept(tk::dot);
      }
      if (dot) {
        auto name = expect(tk::identifier);
        if (peek(tk::lpar) or peek(tk::colon_colon)) {
          auto entity = parse_entity(name.as_identifier());
          expr = parse_function_call(std::move(expr), std::move(entity));
        } else {
          has_question_mark
            = static_cast<bool>(accept(tk::question_mark)) or has_question_mark;
          expr = field_access{
            std::move(expr),
            dot.location,
            has_question_mark,
            name.as_identifier(),
          };
        }
        continue;
      }
      // TODO: We have to differentiate between an operator invocation `foo [0]`
      // and an assignment `foo[0] = 42`. To make an early decision, we for now
      // parse it as an operator if there is whitespace after `foo`.
      // Alternatively, we could see whether we can determine what this has to
      // be from the surrounding context, but that seems a bit brittle.
      if (not trivia_before_next() && peek(tk::lbracket)) {
        auto lbracket = expect(tk::lbracket);
        if (auto rbracket = accept(tk::rbracket)) {
          expr = ast::unpack{
            std::move(expr),
            lbracket.location.combine(rbracket.location),
          };
        } else {
          auto index = parse_expression();
          if (auto comma = accept(tk::comma)) {
            diagnostic::error(
              "found `,` in index expression, which is not a list")
              .primary(comma)
              .throw_();
          }
          rbracket = expect(tk::rbracket);
          const auto has_question_mark
            = static_cast<bool>(accept(tk::question_mark));
          expr = index_expr{
            std::move(expr),   lbracket.location, std::move(index),
            rbracket.location, has_question_mark,
          };
        }
        continue;
      }
      break;
    }
    return expr;
  }

  auto parse_string() -> located<std::string> {
    auto raw = false;
    auto begin = accept(tk::string_begin);
    if (not begin) {
      raw = true;
      begin = expect(tk::raw_string_begin);
    }
    auto content = accept(tk::char_seq);
    auto end = expect(tk::closing_quote);
    auto location = begin.location.combine(end.location);
    auto result = std::invoke([&] {
      if (not content) {
        return std::string{};
      }
      if (raw) {
        validate_string(content.text, location);
        return std::string{content.text};
      }
      return unescape_string(content, location, false);
    });
    return located{std::move(result), location};
  }

  auto parse_blob() -> located<blob> {
    auto raw = false;
    auto begin = accept(tk::blob_begin);
    if (not begin) {
      raw = true;
      begin = expect(tk::raw_blob_begin);
    }
    auto result = blob{};
    if (auto content = accept(tk::char_seq)) {
      if (raw) {
        result = blob{as_bytes(content.text)};
      } else {
        result = unescape_blob(content, false);
      }
    }
    auto end = expect(tk::closing_quote);
    return located{std::move(result), begin.location.combine(end)};
  }

  auto parse_format_expr() -> ast::format_expr {
    auto begin = expect(tk::format_string_begin);
    auto location = begin.location;
    auto segments = std::vector<ast::format_expr::segment>{};
    while (true) {
      if (auto end = accept(tk::closing_quote)) {
        auto location = begin.location.combine(end);
        return ast::format_expr{std::move(segments), location};
      }
      if (auto chars = accept(tk::char_seq)) {
        segments.emplace_back(unescape_string(chars, chars.location, true));
      } else if (accept(tk::fmt_begin)) {
        segments.emplace_back(
          ast::format_expr::replacement{parse_expression()});
        expect(tk::fmt_end);
      } else if (auto fmt_end = accept(tk::fmt_end)) {
        diagnostic::error("found `}}` without prior `{{`")
          .primary(fmt_end)
          .throw_();
      } else {
        TENZIR_ASSERT(eoi());
        diagnostic::error("non-terminated format string")
          .secondary(location, "starts here")
          .primary(next_location())
          .throw_();
      }
    }
  }

  auto parse_primary_expression() -> ast::expression {
    if (accept(tk::lpar)) {
      auto scope = ignore_newlines(true);
      auto result = parse_expression();
      expect(tk::rpar);
      return result;
    }
    if (peek(tk::string_begin) or peek(tk::raw_string_begin)) {
      auto string = parse_string();
      return ast::constant{std::move(string.inner), string.source};
    }
    if (peek(tk::blob_begin) or peek(tk::raw_blob_begin)) {
      auto blob = parse_blob();
      return ast::constant{std::move(blob.inner), blob.source};
    }
    if (peek(tk::format_string_begin)) {
      return parse_format_expr();
    }
    if (auto constant = accept_constant()) {
      return std::move(*constant);
    }
    if (auto token = accept(tk::underscore)) {
      return underscore{token.location};
    }
    if (auto token = accept(tk::dollar_ident)) {
      return dollar_var{token.as_identifier()};
    }
    if (peek(tk::lbrace)) {
      return parse_record_or_pipeline_expr();
    }
    if (peek(tk::lbracket)) {
      return parse_list();
    }
    if (auto at = accept(tk::at)) {
      auto begin = at.location;
      auto ident = expect(tk::identifier).as_identifier();
      auto kind = ast::meta_kind{};
      if (ident.name == "name") {
        kind = ast::meta::name;
      } else if (ident.name == "import_time") {
        kind = ast::meta::import_time;
      } else if (ident.name == "internal") {
        kind = ast::meta::internal;
      } else if (ident.name == "schema") {
        diagnostic::error("use `@name` instead")
          .primary(begin.combine(ident))
          .throw_();
      } else if (ident.name == "schema_id") {
        diagnostic::error("use `type_id(this)` instead")
          .primary(begin.combine(ident))
          .throw_();
      } else {
        diagnostic::error("unknown metadata name `{}`", ident.name)
          .primary(ident)
          .hint("must be one of `name`, `import_time`, `internal`")
          .throw_();
      }
      return ast::meta{kind, begin.combine(ident)};
    }
    if (auto token = accept(tk::this_)) {
      return ast::this_{token.location};
    }
    auto ident = accept(tk::identifier);
    if (not ident) {
      diagnostic::error("expected expression, got {}", next_description())
        .primary(next_location(), "got {}", next_description())
        .throw_();
    }
    auto entity = parse_entity(ident.as_identifier());
    if (peek(tk::lpar)) {
      return parse_function_call({}, std::move(entity));
    }
    if (entity.path.size() != 1) {
      diagnostic::error("expected function call")
        .primary(next_location())
        .throw_();
    }
    auto id = std::move(entity.path.front());
    if (id.name == "type") {
      if (peek(tk::identifier) or peek(tk::lbrace)) {
        return ast::type_expr{
          id.location,
          parse_type_def(),
        };
      }
    }
    auto question_mark = accept(tk::question_mark);
    return ast::root_field{
      std::move(id),
      static_cast<bool>(question_mark),
    };
  }

  auto parse_record_or_pipeline_expr() -> ast::expression {
    auto begin = expect(tk::lbrace);
    auto scope = ignore_newlines(true);
    // TODO: Try to implement this better.
    auto is_record
      = silent_peek(tk::rbrace) || silent_peek(tk::dot_dot_dot)
        || silent_peek(tk::string_begin) || silent_peek(tk::raw_string_begin)
        || (silent_peek(tk::identifier) && silent_peek_n(tk::colon, 1));
    if (is_record) {
      return parse_record(begin.location);
    }
    auto pipe = parse_pipeline();
    auto end = expect(tk::rbrace);
    return pipeline_expr{
      begin.location,
      std::move(pipe),
      end.location,
    };
  }

  /// Unescapes a string or blob without checking for UTF-8 validity.
  auto unescape_unchecked(located<std::string_view> input, bool format_string)
    -> std::string {
    auto result = std::string{};
    auto f = input.inner.begin();
    auto e = input.inner.end();
    for (const auto* it = f; it != e; ++it) {
      auto x = *it;
      if (x != '\\') {
        result.push_back(x);
        if (format_string) {
          if (x == '{') {
            ++it;
            TENZIR_ASSERT(it != e);
            TENZIR_ASSERT(*it == '{');
          } else if (x == '}') {
            ++it;
            TENZIR_ASSERT(it != e);
            TENZIR_ASSERT(*it == '}');
          }
        }
        continue;
      }
      auto backslash = it;
      ++it;
      if (it == e) {
        // This can happen if the file ends with a backslash of a non-terminated
        // format string. Or if we have something like `f"\{x}"`.
        diagnostic::error("backslash is not followed by escape sequence")
          .primary(input.source.subloc(backslash - f, 1))
          .throw_();
      }
      x = *it;
      if (x == '\\') {
        result.push_back('\\');
      } else if (x == '"') {
        result.push_back('"');
      } else if (x == 't') {
        result.push_back('\t');
      } else if (x == 'n') {
        result.push_back('\n');
      } else if (x == 'r') {
        result.push_back('\r');
      } else if (x == 'b') {
        result.push_back('\b');
      } else if (x == 'f') {
        result.push_back('\f');
      } else if (x == 'v') {
        result.push_back('\v');
      } else if (x == 'a') {
        result.push_back('\a');
      } else if (x == '0') {
        result.push_back('\0');
      } else if (x == 'u' or x == 'U') {
        // Unicode escape sequence: \uXXXX or \UXXXXXXXX or \u{...}
        if ((it + 1 != e) and (*(it + 1) == '{')) {
          // Handle \u{...}
          ++it; // now at '{'
          const auto* brace_open = it;
          ++it; // now at first hex digit or '}'
          auto codepoint = uint32_t{0};
          size_t digits = 0;
          bool valid = true;
          while (it != e and *it != '}') {
            auto c = *it;
            codepoint <<= 4;
            if (c >= '0' and c <= '9') {
              codepoint |= (c - '0');
            } else if (c >= 'a' and c <= 'f') {
              codepoint |= (c - 'a' + 10);
            } else if (c >= 'A' and c <= 'F') {
              codepoint |= (c - 'A' + 10);
            } else {
              valid = false;
              break;
            }
            ++digits;
            if (digits > 6) {
              valid = false;
              // We don't break here so that the diagnostic location goes all
              // the way to the closing brace.
            }
            ++it;
          }
          if (not valid or digits == 0 or it == e or *it != '}') {
            diagnostic::error("invalid unicode escape sequence")
              .primary(input.source.subloc(
                brace_open - f, (it - brace_open) + (it != e ? 1 : 0) + 1))
              .throw_();
          }
          // Encode codepoint as UTF-8
          if (codepoint <= 0x7F) {
            result.push_back(static_cast<char>(codepoint));
          } else if (codepoint <= 0x7FF) {
            result.push_back(
              static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else if (codepoint <= 0xFFFF) {
            result.push_back(
              static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else if (codepoint <= 0x10FFFF) {
            result.push_back(
              static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else {
            diagnostic::error("unicode codepoint out of range")
              .primary(input.source.subloc(backslash - f, it + 1 - backslash))
              .throw_();
          }
          // it is at '}', loop will ++it
        } else {
          // Handle \uXXXX or \UXXXXXXXX
          auto digits = (x == 'u') ? 4uz : 8uz;
          if (std::distance(it, e) < static_cast<ptrdiff_t>(digits) + 1) {
            diagnostic::error("incomplete unicode escape sequence")
              .primary(input.source.subloc(backslash - f))
              .throw_();
          }
          auto codepoint = uint32_t{0};
          auto valid = true;
          for (size_t i = 0; i < digits; ++i) {
            ++it;
            TENZIR_ASSERT(it != e);
            auto c = *it;
            codepoint <<= 4;
            if (c >= '0' and c <= '9') {
              codepoint |= (c - '0');
            } else if (c >= 'a' and c <= 'f') {
              codepoint |= (c - 'a' + 10);
            } else if (c >= 'A' and c <= 'F') {
              codepoint |= (c - 'A' + 10);
            } else {
              valid = false;
              break;
            }
          }
          if (not valid) {
            diagnostic::error("invalid unicode escape sequence")
              .primary(input.source.subloc(backslash - f, digits + 2))
              .throw_();
          }
          // Encode codepoint as UTF-8
          if (codepoint <= 0x7F) {
            result.push_back(static_cast<char>(codepoint));
          } else if (codepoint <= 0x7FF) {
            result.push_back(
              static_cast<char>(0xC0 | ((codepoint >> 6) & 0x1F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else if (codepoint <= 0xFFFF) {
            result.push_back(
              static_cast<char>(0xE0 | ((codepoint >> 12) & 0x0F)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else if (codepoint <= 0x10FFFF) {
            result.push_back(
              static_cast<char>(0xF0 | ((codepoint >> 18) & 0x07)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F)));
            result.push_back(
              static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F)));
            result.push_back(static_cast<char>(0x80 | (codepoint & 0x3F)));
          } else {
            diagnostic::error("unicode codepoint out of range")
              .primary(input.source.subloc(backslash - f, digits + 2))
              .throw_();
          }
        }
      } else if (x == 'x') {
        // Hexadecimal byte escape: \xHH
        // We are at `x` now, so the distance to `e` must be at least 3.
        if (std::distance(it, e) < 3) {
          diagnostic::error("incomplete hex escape sequence")
            .primary(input.source.subloc(backslash - f, e - backslash))
            .throw_();
        }
        ++it;
        auto c1 = static_cast<unsigned char>(*it);
        ++it;
        auto c2 = static_cast<unsigned char>(*it);
        auto hex_to_int = [](unsigned char c) -> int {
          if (c >= '0' and c <= '9') {
            return c - '0';
          }
          if (c >= 'a' and c <= 'f') {
            return c - 'a' + 10;
          }
          if (c >= 'A' and c <= 'F') {
            return c - 'A' + 10;
          }
          return -1;
        };
        auto hi = hex_to_int(c1);
        auto lo = hex_to_int(c2);
        if (hi == -1 or lo == -1) {
          diagnostic::error("invalid hex escape sequence")
            .primary(input.source.subloc(backslash - f, 4))
            .throw_();
        }
        result.push_back(static_cast<char>((hi << 4) | lo));
      } else {
        diagnostic::error("found unknown escape sequence `{}`",
                          input.inner.substr(backslash - f, 2))
          .primary(input.source.subloc(backslash - f, 2))
          .throw_();
      }
    }
    return result;
  }

  auto unescape_blob(located<std::string_view> input, bool format_string)
    -> blob {
    return blob{as_bytes(unescape_unchecked(input, format_string))};
  }

  void validate_string(std::string_view content, location location) {
    if (not arrow::util::ValidateUTF8(content)) {
      // TODO: Would be nice to report the actual error location.
      diagnostic::error("string contains invalid utf-8")
        .primary(location)
        .hint("consider using a blob instead: b\"…\"")
        .throw_();
    }
  }

  auto unescape_string(located<std::string_view> content, location location,
                       bool format_string) -> std::string {
    auto unescaped = unescape_unchecked(content, format_string);
    validate_string(unescaped, location);
    return unescaped;
  }

  auto parse_record_item() -> ast::record::item {
    if (auto dots = accept(tk::dot_dot_dot)) {
      return ast::spread{dots.location, parse_expression()};
    }
    // TODO: Use a different representation for field names that are strings or
    // format strings in the AST.
    auto ident = std::invoke([this]() {
      if (auto result = accept(tk::identifier)) {
        return result.as_identifier();
      }
      auto string = parse_string();
      return ast::identifier{string.inner, string.source};
    });
    expect(tk::colon);
    auto expr = parse_expression();
    return ast::record::field{
      std::move(ident),
      std::move(expr),
    };
  }

  auto parse_record(location begin) -> ast::record {
    auto content = std::vector<ast::record::item>{};
    while (not peek(tk::rbrace)) {
      content.emplace_back(parse_record_item());
      if (not peek(tk::rbrace)) {
        expect(tk::comma);
      }
    }
    auto end = expect(tk::rbrace);
    return ast::record{
      begin,
      std::move(content),
      end.location,
    };
  }

  static auto try_double_to_integer(double x) -> constant::kind {
    auto integral = double{};
    auto fractional = std::modf(x, &integral);
    if (fractional != 0.0) {
      return x;
    }
    if (static_cast<double>(min<int64_t>) <= integral
        && integral <= static_cast<double>(max<int64_t>)) {
      return static_cast<int64_t>(x);
    }
    if (static_cast<double>(min<uint64_t>) <= integral
        && integral <= static_cast<double>(max<uint64_t>)) {
      return static_cast<uint64_t>(x);
    }
    return x;
  }

  auto parse_scalar() -> constant {
    // TODO: Accept separator between digits and unit: 1_000_000, 1_h).
    // TODO: The parsers used here don't handle out-of-range (e.g., `50E`).
    auto token = accept(tk::scalar);
    // The following two parsers include the SI unit.
    if (auto result = int64_t{}; parsers::integer(token.text, result)) {
      return constant{result, token.location};
    }
    if (auto result = uint64_t{}; parsers::count(token.text, result)) {
      return constant{result, token.location};
    }
    // Doubles without SI unit shall always remain doubles.
    if (auto result = double{}; parsers::real(token.text, result)) {
      return constant{result, token.location};
    }
    // Otherwise, it might be a double with SI unit, which we try to convert to
    // an integer, such that `1.2k` becomes `1200`.
    if (auto result = double{}; si_parser<double>{}(token.text, result)) {
      return constant{try_double_to_integer(result), token.location};
    }
    if (auto result = duration{};
        parsers::simple_duration(token.text, result)) {
      return constant{result, token.location};
    }
    diagnostic::error("could not parse scalar")
      .primary(token)
      .note("scalar parsing still is very rudimentary")
      .throw_();
  }

  auto accept_constant() -> std::optional<constant> {
    if (peek(tk::scalar)) {
      return parse_scalar();
    }
    // TODO: Provide better error messages here.
    if (auto token = accept(tk::datetime)) {
      if (auto result = time{}; parsers::ymdhms(token.text, result)) {
        return constant{result, token.location};
      }
      diagnostic::error("could not parse datetime").primary(token).throw_();
    }
    if (auto token = accept(tk::true_)) {
      return constant{true, token.location};
    }
    if (auto token = accept(tk::false_)) {
      return constant{false, token.location};
    }
    if (auto token = accept(tk::null)) {
      return constant{caf::none, token.location};
    }
    if (auto token = accept(tk::ip)) {
      if (auto result = ip{}; parsers::ip(token.text, result)) {
        return constant{result, token.location};
      }
      diagnostic::error("could not parse ip address").primary(token).throw_();
    }
    if (auto token = accept(tk::subnet)) {
      if (auto result = subnet{}; parsers::net(token.text, result)) {
        return constant{result, token.location};
      }
      diagnostic::error("could not parse subnet").primary(token).throw_();
    }
    return std::nullopt;
  }

  // TODO: This is just a temporary hack.
  auto parse_multiple_assignments() -> std::vector<ast::assignment> {
    auto result = std::vector<ast::assignment>{};
    if (eoi()) {
      return result;
    }
    while (true) {
      result.push_back(parse_assignment());
      if (eoi()) {
        return result;
      }
      expect(tk::comma);
    }
  }

  auto parse_list() -> ast::list {
    auto begin = expect(tk::lbracket);
    auto scope = ignore_newlines(true);
    auto items = std::vector<ast::list::item>{};
    while (true) {
      if (auto end = accept(tk::rbracket)) {
        return ast::list{
          begin.location,
          std::move(items),
          end.location,
        };
      }
      if (auto dots = accept(tk::dot_dot_dot)) {
        items.emplace_back(ast::spread{dots.location, parse_expression()});
      } else {
        items.emplace_back(parse_expression());
      }
      if (not peek(tk::rbracket)) {
        expect(tk::comma);
      }
    }
  }

  auto parse_function_call(std::optional<ast::expression> subject, entity fn)
    -> function_call {
    expect(tk::lpar);
    auto scope = ignore_newlines(true);
    auto args = std::vector<ast::expression>{};
    auto method = false;
    if (subject) {
      method = true;
      args.push_back(std::move(*subject));
    }
    while (true) {
      if (auto rpar = accept(tk::rpar)) {
        return ast::function_call{
          std::move(fn),
          std::move(args),
          rpar.location,
          method,
        };
      }
      if (auto comma = accept(tk::comma)) {
        if (args.empty()) {
          diagnostic::error("unexpected comma before any arguments")
            .primary(comma)
            .throw_();
        } else {
          diagnostic::error("duplicate comma").primary(comma).throw_();
        }
      }
      args.push_back(parse_expression());
      if (not peek(tk::rpar)) {
        expect(tk::comma);
      }
    }
  }

  auto peek_unary_op() -> std::optional<unary_op> {
#define X(x, y)                                                                \
  if (peek(tk::x)) {                                                           \
    return unary_op::y;                                                        \
  }
    X(move, move);
    X(not_, not_);
    X(minus, neg);
#undef X
    return std::nullopt;
  }

  auto peek_binary_op() -> std::optional<binary_op> {
#define X(x, y)                                                                \
  if (peek(tk::x)) {                                                           \
    return binary_op::y;                                                       \
  }
    X(plus, add);
    X(minus, sub);
    X(star, mul);
    X(slash, div);
    X(greater, gt);
    X(greater_equal, geq);
    X(less, lt);
    X(less_equal, leq);
    X(equal_equal, eq);
    X(bang_equal, neq);
    X(and_, and_);
    X(or_, or_);
    X(in, in);
    X(if_, if_);
    X(else_, else_);
#undef X
    return std::nullopt;
  }

  auto at_pipeline_end() -> bool {
    return eoi() || silent_peek(tk::rbrace);
  }

  auto at_statement_end() -> bool {
    return eoi() || peek(tk::newline) || peek(tk::pipe)
           || silent_peek(tk::rbrace);
  }

  auto accept_stmt_sep() -> bool {
    return accept(tk::newline) || accept(tk::pipe);
  }

  auto token_location(size_t idx) const -> location {
    if (anonymous_) {
      return location::unknown;
    }
    return force_token_location(idx);
  }

  auto force_token_location(size_t idx) const -> location {
    TENZIR_ASSERT(idx < tokens_.size());
    auto begin = idx == 0 ? 0 : tokens_[idx - 1].end;
    auto end = tokens_[idx].end;
    // This ignores `anonymous_` on purpose.
    return location{begin, end};
  }

  auto token_string(size_t idx) const -> std::string_view {
    auto loc = force_token_location(idx);
    return source_.substr(loc.begin, loc.end - loc.begin);
  }

  [[nodiscard]] auto advance() -> location {
    TENZIR_ASSERT(next_ < tokens_.size());
    last_ = next_;
    ++next_;
    consume_trivia();
    tries_.clear();
    return token_location(last_);
  }

  [[nodiscard]] auto accept(token_kind kind) -> accept_result {
    if (next_ < tokens_.size()) {
      if (kind == tokens_[next_].kind) {
        auto loc = advance();
        return accept_result{token_string(last_), loc};
      }
    }
    tries_.push_back(kind);
    return {};
  }

  auto expect(token_kind kind) -> accept_result {
    if (auto result = accept(kind)) {
      return result;
    }
    throw_token();
  }

  auto silent_peek(token_kind kind) -> bool {
    return next_ < tokens_.size() && tokens_[next_].kind == kind;
  }

  auto peek(token_kind kind) -> bool {
    // TODO: Does this count as trying the token?
    tries_.push_back(kind);
    return silent_peek(kind);
  }

  // TODO: Can we get rid of this?
  auto silent_peek_n(token_kind kind, size_t offset = 0) -> bool {
    auto index = next_;
    while (true) {
      if (index >= tokens_.size()) {
        return false;
      }
      if (is_trivia(tokens_[index].kind)) {
        index += 1;
        continue;
      }
      if (offset == 0) {
        return tokens_[index].kind == kind;
      }
      offset -= 1;
      index += 1;
    }
  }

  void set_ignore_newlines(bool value) {
    if (value != ignore_newlines_) {
      ignore_newlines_ = value;
      next_ = last_ + 1;
      consume_trivia();
    }
  }

  class [[nodiscard]] newline_scope {
  public:
    explicit newline_scope(parser* self, bool value)
      : self_{self}, previous_{self->ignore_newlines_} {
      self_->set_ignore_newlines(value);
    }

    void done() {
      if (self_) {
        self_->set_ignore_newlines(previous_);
      }
      self_ = nullptr;
    }

    ~newline_scope() {
      done();
    }

    newline_scope(newline_scope&& other) noexcept
      : self_{other.self_}, previous_{other.previous_} {
      other.self_ = nullptr;
    }

    newline_scope(const newline_scope& other) = delete;
    auto operator=(newline_scope&& other) -> newline_scope& = delete;
    auto operator=(const newline_scope& other) -> newline_scope& = delete;

  private:
    parser* self_;
    bool previous_;
  };

  auto ignore_newlines(bool value) -> newline_scope {
    return newline_scope{this, value};
  }

  auto is_trivia(token_kind kind) const -> bool {
    switch (kind) {
      case tk::line_comment:
      case tk::delim_comment:
      case tk::whitespace:
        return true;
      case tk::newline:
        return ignore_newlines_;
      default:
        return false;
    }
  }

  template <class F>
  void consume_while(F&& f) {
    while (next_ < tokens_.size()) {
      if (f(tokens_[next_].kind)) {
        next_ += 1;
      } else {
        break;
      }
    }
  }

  void consume_trivia() {
    consume_while([&](token_kind k) {
      return is_trivia(k);
    });
  }

  void consume_trivia_with_newlines() {
    consume_while([&](token_kind k) {
      return is_trivia(k) || k == token_kind::newline;
    });
  }

  auto eoi() const -> bool {
    return next_ == tokens_.size();
  }

  auto trivia_before_next() const -> bool {
    // TODO: Is this what we want?
    return next_ > last_ + 1;
  }

  auto next_location() const -> location {
    if (anonymous_) {
      return location::unknown;
    }
    auto loc = location{};
    if (next_ < tokens_.size()) {
      loc.begin = next_ == 0 ? 0 : tokens_[next_ - 1].end;
      loc.end = tokens_[next_].end;
    } else {
      loc.begin = tokens_.back().end;
      loc.end = tokens_.back().end;
    }
    return loc;
  }

  auto next_description() -> std::string_view {
    if (next_ < tokens_.size()) {
      return describe(tokens_[next_].kind);
    }
    return "EOF";
  }

  [[noreturn]] void throw_token(std::string message) {
    diagnostic::error("{}", message)
      .primary(next_location(), "got {}", next_description())
      .throw_();
  }

  [[noreturn]] void throw_token() {
    auto tries = std::exchange(tries_, {});
    std::ranges::sort(tries);
    tries.erase(std::ranges::unique(tries).begin(), tries.end());
    if (tries.empty()) {
      throw_token("internal error: empty expected token set");
    }
    if (tries.size() == 1) {
      throw_token(fmt::format("expected {}, got {}", describe(tries[0]),
                              next_description()));
    }
    auto last = tries.back();
    tries.pop_back();
    throw_token(fmt::format(
      "expected {} or {}",
      fmt::join(tries | std::ranges::views::transform(describe), ", "),
      describe(last)));
  }

  parser(std::span<token> tokens, std::string_view source,
         diagnostic_handler& diag, bool anonymous)
    : tokens_{tokens}, source_{source}, diag_{diag}, anonymous_{anonymous} {
    consume_trivia();
  }

  std::span<token> tokens_;
  std::string_view source_;
  diagnostic_handler& diag_;
  size_t next_ = 0;
  size_t last_ = 0;
  bool ignore_newlines_ = false;
  std::vector<token_kind> tries_;
  bool anonymous_ = false;
};

auto parse_pipeline(std::span<token> tokens, std::string_view source,
                    diagnostic_handler& dh, bool anonymous)
  -> failure_or<ast::pipeline> {
  return parser::parse_with(tokens, source, dh, anonymous, [](parser& self) {
    return self.parse_pipeline();
  });
}

} // namespace

auto parse(std::span<token> tokens, std::string_view source, session ctx)
  -> failure_or<ast::pipeline> {
  return parse_pipeline(tokens, source, ctx, false);
}

auto parse(std::string_view source, session ctx) -> failure_or<ast::pipeline> {
  TRY(auto tokens, tokenize(source, ctx));
  return parse(tokens, source, ctx);
}

auto parse_pipeline_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::pipeline> {
  TRY(auto tokens, tokenize(source, ctx));
  return parse_pipeline(tokens, source, ctx, true);
}

auto parse_expression_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::expression> {
  TRY(auto tokens, tokenize(source, ctx));
  return parser::parse_with(tokens, source, ctx, true, [](class parser& self) {
    return self.parse_expression();
  });
}

auto parse_assignment_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::assignment> {
  TRY(auto tokens, tokenize(source, ctx));
  return parser::parse_with(tokens, source, ctx, true, [](class parser& self) {
    return self.parse_assignment();
  });
}

auto parse_multiple_assignments_with_bad_diagnostics(std::string_view source,
                                                     session ctx)
  -> failure_or<std::vector<ast::assignment>> {
  TRY(auto tokens, tokenize(source, ctx));
  return parser::parse_with(tokens, source, ctx, true, [](class parser& self) {
    return self.parse_multiple_assignments();
  });
}

} // namespace tenzir
