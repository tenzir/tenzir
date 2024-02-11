//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/assert.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/tql2/parser.hpp>

namespace tenzir::tql2::ast {

namespace {

static auto precedence(binary_op x) -> int {
  using enum binary_op;
  switch (x) {
    case star:
    case slash:
      return 2;
    case plus:
    case minus:
      return 1;
    case double_equal:
      return 2;
  }
  TENZIR_UNREACHABLE();
}
} // namespace

class parser {
public:
  using tk = token_kind;

  static auto parse_file(std::span<token> tokens, std::string_view source,
                         diagnostic_handler& diag) -> pipeline {
    try {
      return parser{tokens, source}.parse_pipeline();
    } catch (diagnostic& d) {
      // TODO
      diag.emit(d);
      return pipeline{{}};
    }
  }

private:
  auto accept_stmt_sep() -> bool {
    return accept(tk::newline) || accept(tk::pipe);
  }

  auto parse_pipeline(bool rbrace_end = false) -> pipeline {
    auto scope = ignore_newlines(false);
    auto steps = std::vector<pipeline::step>{};
    while (true) {
      while (accept_stmt_sep()) {
      }
      if (rbrace_end && peek(tk::rbrace)) {
        break;
      }
      // TODO: ??
      if (eoi()) {
        break;
      }
      auto left = parse_selector();
      if (auto equal = accept(tk::equal)) {
        auto right = parse_expression();
        steps.emplace_back(
          assignment{std::move(left), equal.location, std::move(right)});
        if (not accept_stmt_sep() && not eoi()) {
          throw_token();
        }
      } else if (left.path.size() == 1) {
        // TODO: Parse operator.
        auto op = std::move(left.path[0]);
        auto args = std::vector<invocation_arg>{};
        while (not accept_stmt_sep() /*TODO*/ && not peek(tk::rbrace)
               && not eoi()) {
          if (not args.empty()) {
            expect(tk::comma);
            consume_trivia_with_newlines();
          }
          auto expr = parse_expression();
          if (auto equal = accept(tk::equal)) {
            expr.match(
              [&](selector& y) {
                auto left = std::move(y);
                auto right = parse_expression();
                args.emplace_back(assignment{std::move(left), equal.location,
                                             std::move(right)});
              },
              [](auto&) {
                // TODO
                diagnostic::error("left of = must be selector").throw_();
              });
          } else {
            args.emplace_back(std::move(expr));
          }
        }
        steps.emplace_back(invocation{std::move(op), std::move(args)});
      } else {
        TENZIR_TODO();
      }
    }
    return pipeline{std::move(steps)};
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

  auto parse_record_or_pipeline() -> expression {
    auto begin = accept(tk::lbrace);
    if (not begin) {
      throw_token();
    }
    // { }       // unknown -> record
    // { test :  // record
    // OTHERWISE pipeline
    // TODO: This does not respect trivia.
    auto scope = ignore_newlines(true);
    auto is_record = peek(tk::rbrace)
                     || (raw_peek(tk::identifier) && raw_peek(tk::colon, 1));
    if (is_record) {
      auto content = std::vector<record::content_kind>{};
      while (true) {
        if (not content.empty()) {
          (void)accept(tk::comma);
        }
        if (peek(tk::rbrace)) {
          scope.done();
          auto end = expect(tk::rbrace);
          return expression{
            record{begin.location, std::move(content), end.location}};
        }
        auto name = expect(tk::identifier);
        expect(tk::colon);
        auto expr = parse_expression();
        content.emplace_back(
          record::field{identifier{name.text, name.location}, std::move(expr)});
      }
    }
    auto pipe = parse_pipeline(true);
    expect(tk::rbrace);
    return pipe;
  }

  auto parse_atomic_expression() -> expression {
    if (accept(tk::lpar)) {
      auto result = parse_expression();
      expect(tk::rpar);
      return result;
    }
    if (selector_start()) {
      return expression{parse_selector()};
    }
    if (peek(tk::lbrace)) {
      return parse_record_or_pipeline();
    }
    if (auto token = accept(tk::string)) {
      // TODO: Make this better and parse content?
      return expression{
        string{std::string{token.text.substr(1, token.text.size() - 2)},
               token.location}};
    }
    if (auto token = accept(tk::integer)) {
      return expression{integer{token.as_string()}};
    }
    throw_token();
  }

  auto peek_binary_op() -> std::optional<binary_op> {
#define X(x)                                                                   \
  if (auto token = peek(tk::x)) {                                              \
    return binary_op::x;                                                       \
  }
    X(plus);
    X(minus);
    X(star);
    X(slash);
    X(double_equal);
#undef X
    return std::nullopt;
  }

  auto parse_expression(int prec = 0) -> expression {
    // TODO
    auto expr = parse_atomic_expression();
    while (true) {
      if (auto dot = accept(tk::dot)) {
        auto name = expect(tk::identifier);
        expr = field_access{
          std::move(expr),
          dot.location,
          name.as_identifier(),
        };
        continue;
      }
      if (auto bin_op = peek_binary_op()) {
        auto new_prec = precedence(*bin_op);
        if (new_prec >= prec) {
          auto location = advance();
          consume_trivia_with_newlines();
          auto right = parse_expression(new_prec + 1);
          expr = bin_expr{
            std::move(expr),
            located{*bin_op, location},
            std::move(right),
          };
          continue;
        }
      }
      break;
    }
    return expr;
  }

  struct accept_result {
    std::string_view text;
    location location;

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
    TENZIR_ASSERT_CHEAP(next_ < tokens_.size());
    auto begin = next_ == 0 ? 0 : tokens_[next_ - 1].end;
    auto end = tokens_[next_].end;
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

  auto peek(token_kind kind) -> bool {
    // TODO: Does this count as trying the token?
    tries_.push_back(kind);
    return next_ < tokens_.size() && tokens_[next_].kind == kind;
  }

  auto raw_peek(token_kind kind, size_t offset = 0) -> bool {
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

  class [[nodiscard]] newline_scope {
  public:
    explicit newline_scope(bool* ptr, bool value) : previous_{*ptr}, ptr_{ptr} {
      *ptr = value;
    }

    void done() const {
      TENZIR_ASSERT_CHEAP(ptr_);
      *ptr_ = previous_;
    }

    ~newline_scope() {
      if (ptr_) {
        done();
      }
    }

    newline_scope(newline_scope&& other) noexcept : ptr_{other.ptr_} {
      other.ptr_ = nullptr;
    }

    newline_scope(const newline_scope& other) = delete;
    auto operator=(newline_scope&& other) -> newline_scope& = delete;
    auto operator=(const newline_scope& other) -> newline_scope& = delete;

    bool previous_{};
    bool* ptr_ = nullptr;
  };

  auto ignore_newlines(bool value) -> newline_scope {
    auto scope = newline_scope{&ignore_newline_, value};
    consume_trivia();
    return scope;
  }

  auto is_trivia(token_kind kind) const -> bool {
    switch (kind) {
      case tk::line_comment:
      case tk::delim_comment:
      case tk::whitespace:
        return true;
      case tk::newline:
        return ignore_newline_;
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

  [[noreturn]] void throw_token() {
    auto loc = location{};
    auto got = std::string_view{};
    if (next_ < tokens_.size()) {
      loc.begin = next_ == 0 ? 0 : tokens_[next_ - 1].end;
      loc.end = tokens_[next_].end;
      got = to_string(tokens_[next_].kind);
    } else {
      loc.begin = tokens_.back().end;
      loc.end = tokens_.back().end;
      got = "EOF";
    }
    diagnostic::error("expected one of: {}", fmt::join(tries_, ", "))
      .primary(loc, "got {}", got)
      .throw_();
  }

  parser(std::span<token> tokens, std::string_view source)
    : tokens_{tokens}, source_{source} {
    consume_trivia();
  }

  bool ignore_newline_ = false;
  size_t next_ = 0;
  std::vector<token_kind> tries_;
  std::span<token> tokens_;
  std::string_view source_;
};

} // namespace tenzir::tql2::ast

namespace tenzir::tql2 {
auto parse(std::span<token> tokens, std::string_view source,
           diagnostic_handler& diag) -> ast::pipeline {
  return ast::parser::parse_file(tokens, source, diag);
}

} // namespace tenzir::tql2
