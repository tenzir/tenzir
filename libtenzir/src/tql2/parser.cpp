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
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"

#include <arrow/util/utf8.h>

#include <ranges>

namespace tenzir {

namespace {

using namespace ast;

auto precedence(unary_op x) -> int {
  using enum unary_op;
  switch (x) {
    case pos:
    case neg:
      return 7;
    case not_:
      return 3;
  };
  TENZIR_UNREACHABLE();
}

auto precedence(binary_op x) -> int {
  using enum binary_op;
  switch (x) {
    case mul:
    case div:
      return 6;
    case add:
    case sub:
      return 5;
    case gt:
    case geq:
    case lt:
    case leq:
    case eq:
    case neq:
    case in:
      return 4;
    case and_:
      return 2;
    case or_:
      return 1;
  }
  TENZIR_UNREACHABLE();
}

class parser {
public:
  using tk = token_kind;

  static auto parse_file(std::span<const token> tokens, source_ref source,
                         session ctx) -> failure_or<ast::pipeline> {
    try {
      auto self = parser{tokens, std::move(source), ctx};
      auto pipe = self.parse_pipeline();
      if (self.next_ != self.tokens_.size()) {
        self.throw_token("expected EOF");
      }
      return pipe;
    } catch (diagnostic& d) {
      TENZIR_ASSERT(d.severity == severity::error);
      ctx.dh().emit(std::move(d));
      return failure::promise();
    }
  }

private:
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
    expect(tk::if_);
    auto condition = parse_expression();
    expect(tk::lbrace);
    auto consequence = parse_pipeline();
    expect(tk::rbrace);
    auto alternative = std::optional<ast::pipeline>{};
    if (accept(tk::else_)) {
      if (peek(tk::if_)) {
        auto body = std::vector<statement>{};
        body.emplace_back(parse_if_stmt());
        alternative = ast::pipeline{std::move(body)};
      } else {
        expect(tk::lbrace);
        alternative = parse_pipeline();
        expect(tk::rbrace);
      }
    }
    return if_stmt{
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
      filter.push_back(parse_expression());
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
    auto unary_expr = parse_unary_expression();
    if (auto call = std::get_if<ast::function_call>(&*unary_expr.kind)) {
      // TODO: We patch a top-level function call to be an operator invocation
      // instead. This could be done differently by slightly rewriting the
      // parser. Because this is not (yet) reflected in the AST, the optional
      // parenthesis are not reflected.
      if (call->subject) {
        // TODO: We could consider rewriting method calls to mutate their
        // subject, e.g., `foo.bar.baz(qux) => foo.bar = foo.bar.baz(qux)`.
        diagnostic::error("expected operator invocation, found method call")
          .primary(call->fn)
          .throw_();
      }
      if (not at_statement_end()) {
        diagnostic::error("expected end of statement")
          .primary(next_location())
          .throw_();
      }
      return ast::invocation{std::move(call->fn), std::move(call->args)};
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
    auto simple_sel = std::get_if<simple_selector>(&left);
    auto root = simple_sel
                  ? std::get_if<ast::root_field>(&*simple_sel->inner().kind)
                  : nullptr;
    if (root) {
      return parse_invocation(entity{{std::move(root->ident)}});
    }
    diagnostic::error("{}", "expected `=` after selector")
      .primary(next_location())
      .throw_();
  }

  auto parse_invocation(entity op) -> ast::invocation {
    auto args = std::vector<ast::expression>{};
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

  auto to_selector(ast::expression expr) -> selector {
    auto location = expr.get_location();
    auto result = selector::try_from(std::move(expr));
    if (not result) {
      // TODO: Improve error message.
      diagnostic::error("expected selector").primary(location).throw_();
    }
    return std::move(*result);
  }

  auto parse_expression(int min_prec = 0) -> ast::expression {
    auto expr = parse_unary_expression();
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
          continue;
        }
      }
      break;
    }
    // We clear the previously tried token to improve error messages.
    tries_.clear();
    return expr;
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
      if (auto dot = accept(tk::dot)) {
        auto name = expect(tk::identifier);
        if (peek(tk::lpar)) {
          expr = parse_function_call(std::move(expr),
                                     entity{{name.as_identifier()}});
        } else {
          expr = field_access{
            std::move(expr),
            dot.location,
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
          expr = unpack{std::move(expr),
                        lbracket.location.combine(rbracket.location)};
        } else {
          auto index = parse_expression();
          if (auto comma = accept(tk::comma)) {
            diagnostic::error(
              "found `,` in index expression, which is not a list")
              .primary(comma)
              .throw_();
          }
          rbracket = expect(tk::rbracket);
          expr = index_expr{
            std::move(expr),
            lbracket.location,
            std::move(index),
            rbracket.location,
          };
        }
        continue;
      }
      break;
    }
    return expr;
  }

  auto parse_primary_expression() -> ast::expression {
    if (accept(tk::lpar)) {
      auto scope = ignore_newlines(true);
      auto result = parse_expression();
      expect(tk::rpar);
      return result;
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
    // TODO: Drop one of the syntax possibilities.
    if (peek(tk::meta) || peek(tk::at)) {
      auto begin = location{};
      auto is_at = false;
      if (auto meta = accept(tk::meta)) {
        begin = meta.location;
        expect(tk::dot);
      } else {
        begin = expect(tk::at).location;
        is_at = true;
      }
      auto ident = expect(tk::identifier).as_identifier();
      auto kind = ast::meta_kind{};
      if (ident.name == "name") {
        kind = ast::meta::name;
      } else if (ident.name == "import_time") {
        kind = ast::meta::import_time;
      } else if (ident.name == "internal") {
        kind = ast::meta::internal;
      } else if (ident.name == "schema") {
        diagnostic::error("use `{}name` instead", is_at ? "@" : "meta.")
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
    auto path = std::vector<ast::identifier>{};
    path.push_back(ident.as_identifier());
    while (accept(tk::single_quote)) {
      path.push_back(expect(tk::identifier).as_identifier());
    }
    if (peek(tk::lpar)) {
      return parse_function_call({}, ast::entity{std::move(path)});
    }
    if (path.size() != 1) {
      diagnostic::error("expected function call")
        .primary(next_location())
        .throw_();
    }
    return ast::root_field{std::move(path[0])};
  }

  auto parse_record_or_pipeline_expr() -> ast::expression {
    auto begin = expect(tk::lbrace);
    auto scope = ignore_newlines(true);
    // TODO: Try to implement this better.
    auto is_record
      = silent_peek(tk::rbrace) || silent_peek(tk::dot_dot_dot)
        || ((silent_peek(tk::string) || silent_peek(tk::identifier))
            && silent_peek_n(tk::colon, 1));
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

  auto parse_record_item() -> ast::record::item {
    if (accept(tk::dot_dot_dot)) {
      return ast::record::spread{parse_expression()};
    }
    auto name = accept(tk::identifier);
    if (not name) {
      // TODO: Decide how to represent string fields in the AST.
      name = expect(tk::string);
      name.text = name.text.substr(1, name.text.size() - 2);
    }
    expect(tk::colon);
    auto expr = parse_expression();
    return ast::record::field{
      name.as_identifier(),
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

  auto parse_string() -> constant {
    auto token = expect(tk::string);
    // TODO: Implement this properly.
    auto result = std::string{};
    TENZIR_ASSERT(token.text.size() >= 2);
    TENZIR_ASSERT(token.text.front() == '"');
    TENZIR_ASSERT(token.text.back() == '"');
    auto f = token.text.begin() + 1;
    auto e = token.text.end() - 1;
    for (auto it = f; it != e; ++it) {
      auto x = *it;
      if (x != '\\') {
        result.push_back(x);
        continue;
      }
      ++it;
      if (it == e) {
        // TODO: invalid, but cannot happen
        TENZIR_UNREACHABLE();
      }
      x = *it;
      if (x == '\\') {
        result.push_back('\\');
      } else if (x == '"') {
        result.push_back('"');
      } else if (x == 'n') {
        result.push_back('\n');
      } else if (x == '0') {
        result.push_back('\0');
      } else {
        diagnostic::error("found unknown escape sequence `{}`",
                          token.text.substr(it - f, 2))
          .primary(token.location.subloc(it - f, 2))
          .throw_();
      }
    }
    if (not arrow::util::ValidateUTF8(result)) {
      // TODO: Would be nice to report the actual error location.
      diagnostic::error("string contains invalid utf-8")
        .primary(token)
        .hint("consider using a blob instead: b{}", token.text)
        .throw_();
    }
    return constant{std::move(result), token.location};
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
    if (peek(tk::string)) {
      return parse_string();
    }
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

  auto parse_list() -> ast::list {
    auto begin = expect(tk::lbracket);
    auto scope = ignore_newlines(true);
    auto items = std::vector<ast::expression>{};
    while (true) {
      if (auto end = accept(tk::rbracket)) {
        return ast::list{
          begin.location,
          std::move(items),
          end.location,
        };
      }
      items.push_back(parse_expression());
      if (not peek(tk::rbracket)) {
        expect(tk::comma);
      }
    }
  }

  auto parse_function_call(std::optional<ast::expression> subject,
                           entity fn) -> function_call {
    expect(tk::lpar);
    auto scope = ignore_newlines(true);
    auto args = std::vector<ast::expression>{};
    while (true) {
      if (auto rpar = accept(tk::rpar)) {
        return ast::function_call{
          std::move(subject),
          std::move(fn),
          std::move(args),
          rpar.location,
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

  struct accept_result {
    std::string_view text;
    tenzir::location location;

    explicit operator bool() const {
      // TODO: Is this okay?
      return text.data() != nullptr;
    }

    auto as_identifier() const -> ast::identifier {
      return ast::identifier{text, location};
    }

    auto as_string() const -> located<std::string> {
      return {text, location};
    }

    auto get_location() const -> tenzir::location {
      return location;
    }
  };

  [[nodiscard]] auto advance() -> location {
    TENZIR_ASSERT(next_ < tokens_.size());
    auto begin = next_ == 0 ? 0 : tokens_[next_ - 1].end;
    auto end = tokens_[next_].end;
    last_ = next_;
    ++next_;
    consume_trivia();
    tries_.clear();
    return {source_ref_.borrow(), begin, end};
  }

  [[nodiscard]] auto accept(token_kind kind) -> accept_result {
    if (next_ < tokens_.size()) {
      if (kind == tokens_[next_].kind) {
        auto loc = advance();
        return accept_result{source_.substr(loc.begin, loc.end - loc.begin),
                             location{source_ref_.borrow(), loc.begin,
                                      loc.end}};
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

  auto next_location() -> location {
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

  parser(std::span<const token> tokens, source_ref source, session ctx)
    : tokens_{tokens},
      source_{ctx.source_map().get(source.borrow()).text},
      source_ref_{std::move(source)},
      ctx_{ctx} {
    consume_trivia();
  }

  std::span<const token> tokens_;
  std::string_view source_;
  source_ref source_ref_;
  session ctx_;
  size_t next_ = 0;
  size_t last_ = 0;
  bool ignore_newlines_ = false;
  std::vector<token_kind> tries_;
};

} // namespace

auto parse(tokens tokens, session ctx) -> failure_or<ast::pipeline> {
  return parser::parse_file(tokens.items, std::move(tokens.source), ctx);
}

} // namespace tenzir
