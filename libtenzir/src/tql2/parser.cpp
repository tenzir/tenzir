//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/parser.hpp"

#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/detail/assert.hpp"

#include <arrow/util/utf8.h>

#include <ranges>

namespace tenzir::tql2 {

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
    case ge:
    case lt:
    case le:
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

  static auto parse_file(std::span<token> tokens, std::string_view source,
                         diagnostic_handler& diag) -> pipeline {
    try {
      auto self = parser{tokens, source, diag};
      auto pipe = self.parse_pipeline();
      if (self.next_ != self.tokens_.size()) {
        self.throw_token("expected EOF");
      }
      return pipe;
    } catch (diagnostic& d) {
      // TODO
      diag.emit(d);
      return pipeline{{}};
    }
  }

private:
  auto parse_pipeline() -> pipeline {
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
    return pipeline{std::move(body)};
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
        .primary(ident.location, "try `${}` instead", ident.text)
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
    auto alternative = std::optional<pipeline>{};
    if (accept(tk::else_)) {
      expect(tk::lbrace);
      alternative = parse_pipeline();
      expect(tk::rbrace);
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
    expect(tk::match);
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
    expect(tk::rbrace);
    return match_stmt{
      std::move(expr),
      std::move(arms),
    };
  }

  auto parse_match_stmt_arm() -> match_stmt::arm {
    auto filter = std::vector<expression>{};
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
    auto left = parse_selector();
    if (auto equal = accept(tk::equal)) {
      auto right = parse_expression();
      return assignment{
        std::move(left),
        equal.location,
        std::move(right),
      };
    }
    // TODO: Proper entity parsing.
    if (not left.this_ and left.path.size() == 1) {
      return parse_invocation(entity{std::move(left.path)});
    }
    diagnostic::error("expected operator name or `=` afterwards")
      .primary(left.get_location())
      .throw_();
  }

  auto parse_invocation(entity op) -> invocation {
    auto args = std::vector<expression>{};
    while (not at_statement_end()) {
      if (not args.empty()) {
        if (not accept(tk::comma)) {
          // Allow `{ ... }` without comma as final argument.
          if (not peek(tk::lbrace)) {
            diagnostic::error("unexpected continuation of arguments")
              .primary(next_location())
              .hint("try inserting a `,` before")
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
    return invocation{
      std::move(op),
      std::move(args),
    };
  }

  auto parse_expression(int min_prec = 0) -> expression {
    auto expr = parse_unary_expression();
    while (true) {
      if (min_prec == 0) {
        if (auto equal = accept(tk::equal)) {
          auto left = std::get_if<selector>(expr.kind.get());
          if (not left) {
            diagnostic::error("left of `=` must be selector")
              .primary(expr.get_location())
              .hint("equality comparison is done with `==`")
              .throw_();
          }
          // TODO: Check precedence.
          auto right = parse_expression();
          expr = assignment{
            std::move(*left),
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
    // TODO: Does this improve error messages?
    tries_.clear();
    return expr;
  }

  auto parse_unary_expression() -> expression {
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
      } else if (auto lbracket = accept(tk::lbracket)) {
        if (auto rbracket = accept(tk::rbracket)) {
          expr = unpack{std::move(expr),
                        lbracket.location.combine(rbracket.location)};
        } else {
          auto index = parse_expression();
          rbracket = expect(tk::rbracket);
          expr = index_expr{
            std::move(expr),
            lbracket.location,
            std::move(index),
            rbracket.location,
          };
        }
      } else {
        break;
      }
    }
    return expr;
  }

  auto parse_primary_expression() -> expression {
    if (accept(tk::lpar)) {
      auto scope = ignore_newlines(true);
      auto result = parse_expression();
      expect(tk::rpar);
      return result;
    }
    if (auto lit = accept_literal()) {
      return std::move(*lit);
    }
    if (auto token = accept(tk::underscore)) {
      return underscore{};
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
    // TODO: Accept entity as function name.
    if (not selector_start()) {
      // TODO: This is maybe a bit hacky?
      diagnostic::error("expected expression, got {}", next_description())
        .primary(next_location(), "got {}", next_description())
        .throw_();
    }
    // TODO: The code below is a mess and needs to be improved.
    auto sel = parse_selector();
    auto ent = std::optional<entity>{};
    if (accept(tk::single_quote)) {
      if (sel.this_ || sel.path.size() != 1) {
        diagnostic::error("todo: unexpected stuff before entity")
          .primary(sel.get_location())
          .throw_();
      }
      auto path = std::move(sel.path);
      while (true) {
        auto ident = expect(tk::identifier);
        path.push_back(ident.as_identifier());
        if (not accept(tk::single_quote)) {
          break;
        }
      }
      ent = entity{std::move(path)};
    }
    if (peek(tk::lpar)) {
      // TODO: Consider refactoring this.
      auto subject = std::optional<expression>{};
      if (not ent) {
        if (sel.this_ and sel.path.empty()) {
          diagnostic::error("`this` cannot be called")
            .primary(*sel.this_)
            .throw_();
        }
        TENZIR_ASSERT(not sel.path.empty());
        ent = entity{{std::move(sel.path.back())}};
        sel.path.pop_back();
        if (sel.this_ || not sel.path.empty()) {
          subject = std::move(sel);
        }
      }
      TENZIR_ASSERT(ent);
      return parse_function_call(std::move(subject), std::move(*ent));
    }
    if (ent) {
      diagnostic::error("todo: referenced entity that is not a function call")
        .primary(ent->get_location())
        .throw_();
    }
    return sel;
  }

  auto selector_start() -> bool {
    return peek(tk::identifier) || peek(tk::this_);
  }

  auto parse_selector() -> selector {
    auto this_ = accept(tk::this_);
    auto path = std::vector<identifier>{};
    while (true) {
      if (this_ || not path.empty()) {
        if (not accept(tk::dot)) {
          break;
        }
      }
      if (auto ident = accept(tk::identifier)) {
        path.emplace_back(std::string{ident.text}, ident.location);
      } else {
        throw_token();
      }
    }
    return selector{this_ ? this_.location : std::optional<location>{},
                    std::move(path)};
  }

  auto parse_record_or_pipeline_expr() -> expression {
    auto begin = expect(tk::lbrace);
    auto scope = ignore_newlines(true);
    // TODO: Try to implement this better.
    auto is_record
      = silent_peek(tk::rbrace)
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

  auto parse_record(location begin) -> record {
    auto content = std::vector<record::content_kind>{};
    while (not peek(tk::rbrace)) {
      // TODO: Parse `{ foo: 42, ...bar }`.
      auto name = accept(tk::identifier);
      if (not name) {
        // TODO: Decide how to represent string fields in the AST.
        name = expect(tk::string);
        name.text = name.text.substr(1, name.text.size() - 2);
      }
      expect(tk::colon);
      auto expr = parse_expression();
      content.emplace_back(record::field{
        name.as_identifier(),
        std::move(expr),
      });
      if (not peek(tk::rbrace)) {
        expect(tk::comma);
      }
    }
    auto end = expect(tk::rbrace);
    return record{
      begin,
      std::move(content),
      end.location,
    };
  }

  auto parse_string() -> literal {
    auto token = expect(tk::string);
    // TODO: Implement this properly.
    auto result = std::string{};
    TENZIR_ASSERT(token.text.size() >= 2);
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
        .primary(token.location)
        .hint("consider using a blob instead: b{}", token.text)
        .throw_();
    }
    return literal{std::move(result), token.location};
  }

  auto parse_scalar() -> literal {
    auto token = accept(tk::scalar);
    // TODO: Make this better, do not use existing parsers.
    if (auto result = int64_t{}; parsers::i64(token.text, result)) {
      return literal{result, token.location};
    }
    if (auto result = uint64_t{}; parsers::u64(token.text, result)) {
      return literal{result, token.location};
    }
    if (auto result = double{}; parsers::real(token.text, result)) {
      return literal{result, token.location};
    }
    if (auto result = duration{}; parsers::duration(token.text, result)) {
      return literal{result, token.location};
    }
    diagnostic::error("could not parse scalar")
      .primary(token.location)
      .note("scalar parsing still is very rudimentary")
      .throw_();
  }

  auto accept_literal() -> std::optional<literal> {
    if (peek(tk::string)) {
      return parse_string();
    }
    if (peek(tk::scalar)) {
      return parse_scalar();
    }
    if (auto token = accept(tk::datetime)) {
      // TODO: Make this better.
      if (auto result = time{}; parsers::ymdhms(token.text, result)) {
        return literal{result, token.location};
      }
      diagnostic::error("could not parse datetime")
        .primary(token.location)
        .throw_();
    }
    if (auto token = accept(tk::true_)) {
      return literal{true, token.location};
    }
    if (auto token = accept(tk::false_)) {
      return literal{false, token.location};
    }
    if (auto token = accept(tk::null)) {
      return literal{null{}, token.location};
    }
    if (auto token = accept(tk::ip)) {
      // TODO
      if (auto result = ip{}; parsers::ip(token.text, result)) {
        return literal{result, token.location};
      }
    }
    return std::nullopt;
  }

  auto parse_list() -> list {
    auto begin = expect(tk::lbracket);
    auto scope = ignore_newlines(true);
    auto items = std::vector<expression>{};
    while (true) {
      if (auto end = accept(tk::rbracket)) {
        return list{
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

  auto parse_function_call(std::optional<expression> subject, entity fn)
    -> function_call {
    expect(tk::lpar);
    auto scope = ignore_newlines(true);
    auto args = std::vector<expression>{};
    while (not accept(tk::rpar)) {
      if (auto comma = accept(tk::comma)) {
        if (args.empty()) {
          diagnostic::error("unexpected comma before any arguments")
            .primary(comma.location)
            .throw_();
        } else {
          diagnostic::error("duplicate comma").primary(comma.location).throw_();
        }
      }
      args.push_back(parse_expression());
      if (not peek(tk::rpar)) {
        expect(tk::comma);
      }
    }
    return function_call{
      std::move(subject),
      std::move(fn),
      std::move(args),
    };
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
    X(greater_equal, ge);
    X(less, le);
    X(less_equal, le);
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

    auto as_identifier() const -> identifier {
      return identifier{text, location};
    }

    auto as_string() const -> located<std::string> {
      return {text, location};
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
    return {begin, end};
  }

  [[nodiscard]] auto accept(token_kind kind) -> accept_result {
    if (next_ < tokens_.size()) {
      if (kind == tokens_[next_].kind) {
        auto loc = advance();
        return accept_result{source_.substr(loc.begin, loc.end - loc.begin),
                             location{loc.begin, loc.end}};
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

  auto silent_peek_n(token_kind kind, size_t offset = 0) -> bool {
    // TODO: Can we get rid of this?
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

  parser(std::span<token> tokens, std::string_view source,
         diagnostic_handler& diag)
    : tokens_{tokens}, source_{source}, diag_{diag} {
    consume_trivia();
  }

  std::span<token> tokens_;
  std::string_view source_;
  diagnostic_handler& diag_;
  size_t next_ = 0;
  size_t last_ = 0;
  bool ignore_newlines_ = false;
  std::vector<token_kind> tries_;
};

} // namespace

auto parse(std::span<token> tokens, std::string_view source,
           diagnostic_handler& diag) -> ast::pipeline {
  return parser::parse_file(tokens, source, diag);
}

} // namespace tenzir::tql2
