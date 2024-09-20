//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql/parser.hpp"

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/concept/parseable/tenzir/expression.hpp"
#include "tenzir/concept/parseable/tenzir/pipeline.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/parser_interface.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql/expression.hpp"
#include "tenzir/type.hpp"

#include <caf/detail/scope_guard.hpp>

#include <iterator>

namespace tenzir::tql {

namespace {

// TODO: Consider a proper lexer (based on maximal-munch). The lexer (and more
// or less the whole parser) was built to get the job done for now, but has to
// be improved soon.
template <class T>
struct lexer_traits;

template <>
struct lexer_traits<identifier> {
  static auto parser() {
    // TODO: This clearly shows the weakness of the current approach.
    // TODO: The plugin parser in
    //       `include/tenzir/concept/parseable/tenzir/identifier.hpp`
    //       needs to get aligned with this.
    using namespace parsers;
    return ((alpha | chr{'_'}) >> *(alnum | chr{'_'} | chr{'-'})) //
           - "true" - "false";
  }

  static auto build(std::string parsed, location source) {
    return identifier{std::move(parsed), source};
  }
};

template <>
struct lexer_traits<binary_op> {
  static auto parser() {
    using parsers::str;
    return str{"=="} | str{"!="} | str{"+"} | str{"*"};
  }

  static auto build(std::string parsed, location source)
    -> std::pair<binary_op, location> {
    if (parsed == "==")
      return {binary_op::equals, source};
    if (parsed == "!=")
      return {binary_op::not_equals, source};
    if (parsed == "+")
      return {binary_op::add, source};
    if (parsed == "*")
      return {binary_op::mul, source};
    TENZIR_UNREACHABLE();
  }
};

template <>
struct lexer_traits<int64_t> {
  static auto parser() {
    return parsers::integer;
  }

  static auto build(int64_t parsed, location source)
    -> std::pair<int64_t, location> {
    return {parsed, source};
  }
};

template <>
struct lexer_traits<bool> {
  static auto parser() {
    return parsers::boolean;
  }

  static auto build(bool parsed, location source) -> expression {
    return {parsed, source};
  }
};

auto get_aliases_global()
  -> std::pair<std::mutex, std::unordered_map<std::string, std::string>>& {
  static auto x
    = std::pair<std::mutex, std::unordered_map<std::string, std::string>>{};
  return x;
}

auto resolve_alias(const std::string& name) -> std::optional<std::string> {
  auto& [mutex, map] = get_aliases_global();
  auto lock = std::unique_lock{mutex};
  auto it = map.find(name);
  if (it == map.end()) {
    return std::nullopt;
  }
  return it->second;
}

class parser final : public parser_interface {
private:
  [[nodiscard]] auto legacy_accept(auto p) {
    advance_to_token();
    return to_parser(p).apply(current_, end_);
  }

  [[nodiscard]] auto accept_with_span(auto p) {
    advance_to_token();
    auto begin = current_pos();
    auto result = to_parser(p).apply(current_, end_);
    if (result) {
      return std::optional{
        std::pair{std::move(*result), location::legacy(begin, current_pos())}};
    }
    return std::optional<
      std::pair<std::remove_reference_t<decltype(*result)>, location>>{};
  }

  template <class T>
  [[nodiscard]] auto accept() {
    auto parsed = accept_with_span(lexer_traits<T>::parser());
    if (parsed) {
      return std::optional{
        lexer_traits<T>::build(std::move(parsed->first), parsed->second)};
    }
    return std::optional<decltype(lexer_traits<T>::build(
      std::move(parsed->first), parsed->second))>{};
  }

  [[nodiscard]] auto accept(std::string_view x) -> std::optional<location> {
    auto impl = [&](auto p) -> std::optional<location> {
      if (auto result = accept_with_span(p))
        return result->second;
      return {};
    };
    if (x == "=") {
      return impl('=' >> !parsers::chr{'='});
    }
    if (x == "if") {
      // TODO: Hacky.
      return impl("if" >> !(parsers::alnum | '_'));
    }
    return impl(std::string{x});
  }

  [[nodiscard]] auto accept(char x) -> std::optional<location> {
    return accept(std::string_view{&x, 1});
  }

  template <class T>
  auto peek() {
    return rollback([&] {
      return accept<T>();
    });
  }

  template <class T>
  auto peek(T&& x) {
    return rollback([&] {
      return accept(std::forward<T>(x));
    });
  }

  template <typename F>
  auto rollback(F&& f) {
    auto guard = caf::detail::scope_guard{[this, previous = current_] {
      current_ = previous;
    }};
    return std::forward<F>(f)();
  }

  [[nodiscard]] auto legacy_peek(auto p) {
    return rollback([&] {
      return legacy_accept(std::move(p));
    });
  }

public:
  auto accept_identifier() -> std::optional<identifier> override {
    return accept<identifier>();
  }

  auto peek_identifier() -> std::optional<identifier> override {
    return peek<identifier>();
  }

  auto accept_equals() -> std::optional<location> override {
    if (auto result = accept_with_span("=" >> !parsers::chr{'='})) {
      return result->second;
    }
    return {};
  }

  auto parse_expression() -> expression override {
    return parse_expr_prec(0);
  }

  auto accept_shell_arg() -> std::optional<located<std::string>> override {
    if (auto arg = accept_with_span(parsers::operator_arg)) {
      return located<std::string>{std::move(arg->first), arg->second};
    }
    return {};
  }

  auto peek_shell_arg() -> std::optional<located<std::string>> override {
    return rollback([&] {
      return accept_shell_arg();
    });
  }

  auto parse_legacy_expression() -> located<tenzir::expression> override {
    if (auto result = accept_with_span(parsers::expr)) {
      return {std::move(result->first), result->second};
    }
    throw_at_current("could not parse legacy expression");
  }

  auto parse_extractor() -> extractor override {
    // We can optionally start with: * . field :type
    // Afterwards, we can have: .* .field [0] :type
    auto path = std::vector<projection>{};
    auto start = next_pos();
    auto next_is_first = true;
    while (true) {
      auto first = std::exchange(next_is_first, false);
      if (auto left_bracket = accept('[')) {
        if (first) {
          diagnostic::error("expected `.` before `[`")
            .primary(*left_bracket)
            .throw_();
        }
        diagnostic::error("indexing is not yet implemented")
          .primary(*left_bracket)
          .throw_();
      } else if (auto colon = accept(':')) {
        if (auto type_name = accept("int64")) {
          path.emplace_back(
            located<type>{type{int64_type{}},
                          location::legacy(colon->begin, type_name->end)});
        } else {
          throw_at_current("unknown type name after `:`");
        }
      } else {
        auto dot = accept('.');
        if (dot || first) {
          if (auto star = accept('*')) {
            path.emplace_back(star_projection{*star});
          } else if (auto ident = accept<identifier>()) {
            path.emplace_back(std::move(*ident));
          } else if (first) {
            if (dot) {
              // We got here because the extractor starts with `.`, but the
              // following element is not an identifier or `*`. However, it
              // can start with `.[`, hence we have to check for this.
              if (peek('[')) {
                continue;
              }
              // Now only the root extractor `.` remains.
              TENZIR_DIAG_ASSERT(path.empty());
              return extractor{{}, *dot};
            }
            throw_at_current("expected extractor");
          } else {
            TENZIR_DIAG_ASSERT(dot);
            throw_at_current("expected `*` or identifier");
          }
        } else {
          TENZIR_DIAG_ASSERT(!path.empty());
          auto source = location::legacy(start, path.back().source().end);
          return extractor{std::move(path), source};
        }
      }
    }
  }

  auto parse_data() -> located<tenzir::data> override {
    if (auto result = accept_with_span(parsers::data)) {
      return {std::move(result->first), result->second};
    }
    throw_at_current("could not parse data");
  }

  auto parse_int() -> located<int64_t> override {
    if (auto result = accept_with_span(parsers::i64)) {
      return {std::move(result->first), result->second};
    }
    throw_at_current("could not parse integer");
  }

  auto accept_char(char c) -> std::optional<location> override {
    return accept(c);
  }

  auto peek_char(char c) -> std::optional<location> override {
    return peek(c);
  }

  auto at_end() -> bool override {
    return rollback([&] {
      return !!accept_statement_end();
    });
  }

  auto current_span() -> location override {
    if (internal_) {
      return {};
    }
    return location::legacy(current_pos(), current_pos() + 1);
  }

private:
  auto parse_pipeline() -> std::vector<located<operator_ptr>> {
    if (legacy_accept(parsers::eoi)) {
      return {};
    }
    auto result = std::vector<located<operator_ptr>>{};
    while (true) {
      result.push_back(parse_operator());
      if (!accept_operator_sep()) {
        if (legacy_accept(parsers::eoi)) {
          break;
        }
        throw_at_current("expected end of operator here");
      }
    }
    return result;
  }

  auto parse_operator() -> located<operator_ptr> override {
    // TODO: Where to put parse statement end?
    if (auto name = accept<identifier>()) {
      return parse_operator(std::move(*name));
    }
    throw_at_current("expected operator name");
  }

  auto parse_operator(identifier ident) -> located<operator_ptr> {
    auto const* plugin = plugins::find_operator(ident.name);
    if (auto definition = resolve_alias(ident.name)) {
      if (plugin) {
        diagnostic::error(
          "ambiguous operator: `{}` is a plugin, but also an alias", ident.name)
          .primary(ident.source)
          .throw_();
      }
      auto copy = recursed_;
      auto inserted = copy.emplace(ident.name).second;
      if (!inserted) {
        diagnostic::error("operator `{}` is self-recursive", ident.name)
          .primary(ident.source)
          .throw_();
      }
      auto result
        = parser{*definition, diag_, true, std::move(copy)}.parse_pipeline();
      auto pipe = std::make_unique<pipeline>(to_pipeline(std::move(result)));
      return located<operator_ptr>{std::move(pipe), ident.source};
    }
    if (!plugin) {
      diagnostic::error("no such operator: `{}`", ident.name)
        .primary(ident.source)
        .docs("https://docs.tenzir.com/operators")
        .throw_();
    }
    try {
      // TODO: Replace check with assert.
      if (auto op = plugin->parse_operator(*this)) {
        return {
          std::move(op),
          location::legacy(ident.source.begin, current_pos()),
        };
      }
    } catch (const diagnostic& diag) {
      // Forward diagnostic errors.
      throw;
    } catch (...) {
      diagnostic::error("internal error: {} operator "
                        "threw unexpected exception",
                        ident.name)
        .primary(ident.source)
        .throw_();
    }
    // TODO: Remove this legacy fallback.
    auto [rest, op] = plugin->make_operator({current_, end_});
    auto op_end = rest.data();
    while (*(op_end - 1) == ' ' || *(op_end - 1) == '|') {
      --op_end;
    }
    auto source = location::unknown;
    if (!internal_) {
      source.begin = ident.source.begin;
      source.end = detail::narrow<size_t>(op_end - source_.data());
    }
    if (!op) {
      diagnostic::error("could not parse `{}` operator", ident.name)
        .primary(source)
        .note(fmt::to_string(op.error()))
        .throw_();
    }
    current_ = op_end;
    return {std::move(*op), source};
  }

  auto parse_primary_expr() -> expression {
    auto start = current_;
    if (auto result = parsers::data.apply(current_, end_)) {
      // The current `data` parser is too greedy and parses `true_or_false` as
      // `true`. We thus discard its result if an identifier character follows.
      if (current_ != end_ && (std::isalnum(*current_) || *current_ == '_')) {
        current_ = start;
      } else {
        auto source = location::unknown;
        if (!internal_) {
          source.begin = detail::narrow<size_t>(start - source_.data());
          source.end = detail::narrow<size_t>(current_ - source_.data());
        }
        return expression{std::move(*result), source};
      }
    }
    if (peek<identifier>()) {
      auto extr = parse_extractor();
      if (extr.path.size() == 1 && legacy_accept("(")) {
        // TODO: Make this better, remove assertion.
        auto ident_ptr = std::get_if<identifier>(&extr.path[0]);
        TENZIR_DIAG_ASSERT(ident_ptr);
        auto ident = std::move(*ident_ptr);
        auto args = std::vector<expression>{};
        while (true) {
          auto closing = accept(")");
          if (closing) {
            auto source = location::legacy(ident.source.begin, closing->end);
            return expression{call_expr{std::move(ident), std::move(args)},
                              source};
          }
          if (!args.empty() && !legacy_accept(",")) {
            throw_at_current("expected `,` or `)`");
          }
          args.push_back(parse_expression());
        }
      }
      auto source = extr.source;
      return {std::move(extr), source};
    }
    // TODO
    // if (auto source = accept(":int64")) {
    //   return {extractor{{type{int64_type{}}}, *source}, *source};
    // }
    // if (auto source = accept(":ip")) {
    //   return {extractor{{type{ip_type{}}}, *source}, *source};
    // }
    if (auto x = accept("#schema")) {
      return {meta_extractor::schema, *x};
    }
    // if (auto x = accept2<bool>()) {
    //   return *x;
    // }
    // if (auto integer = accept2<int64_t>()) {
    //   return {integer->first, integer->second};
    // }
    if (auto open_par = accept('(')) {
      auto expr = parse_expression();
      if (!accept(')')) {
        diagnostic::error("missing closing parenthesis")
          .primary(current_span(), "expected `)`")
          .secondary(*open_par, "matching this `(`")
          .throw_();
      }
      return expr;
    }
    throw_at_current("expected expression");
  }

  template <class... Ts>
  [[noreturn]] void
  throw_at_current(fmt::format_string<Ts...> str, Ts&&... xs) {
    diagnostic::error(std::move(str), std::forward<Ts>(xs)...)
      .primary(current_span())
      .throw_();
  }

  auto precedence(binary_op op) const -> int {
    switch (op) {
      using enum binary_op;
      case equals:
        return 1;
      case not_equals:
        return 2;
      case add:
        return 3;
      case mul:
        return 4;
    }
    TENZIR_UNREACHABLE();
  }

  auto left_associative(binary_op op) const -> bool {
    switch (op) {
      using enum binary_op;
      case equals:
      case not_equals:
      case add:
      case mul:
        return true;
    }
    TENZIR_UNREACHABLE();
  }

  auto parse_expr_prec(int min_precedence) -> expression {
    auto lhs = parse_primary_expr();
    while (auto op = peek<binary_op>()) {
      auto op_prec = precedence(op->first);
      if (op_prec < min_precedence) {
        break;
      }
      TENZIR_DIAG_ASSERT(accept<binary_op>() == op);
      auto rhs
        = parse_expr_prec(left_associative(op->first) ? op_prec + 1 : op_prec);
      lhs = expression{binary_expr{std::move(lhs), op->first, op->second,
                                   std::move(rhs)},
                       location::legacy(lhs.source.begin, rhs.source.end)};
    }
    return lhs;
  }

  [[nodiscard]] auto accept_statement_end() -> std::optional<location> {
    if (auto x = accept_with_span('|' | parsers::eoi)) {
      return x->second;
    }
    return {};
  }

  [[nodiscard]] auto accept_operator_sep() -> std::optional<location> {
    if (auto x = accept_with_span('|')) {
      return x->second;
    }
    return {};
  }

  auto accept_integer() -> std::optional<expression> {
    if (auto result = accept_with_span(parsers::i64)) {
      return expression{result->first, result->second};
    }
    return {};
  }

  void advance_to_token() {
    auto line_comment = (parsers::str{"//"} | ("#" >> !parsers::alpha))
                        >> (*(parsers::any - '\n'));
    auto multiline_comment
      = "/*" >> *(parsers::any - (parsers::chr{'*'} >> &parsers::chr{'/'}))
        >> "*/";
    TENZIR_DIAG_ASSERT((*(parsers::space | line_comment | multiline_comment))
                         .apply(current_, end_));
  }

  auto next_pos() -> size_t {
    advance_to_token();
    return current_pos();
  }

  auto current_pos() const -> size_t {
    if (internal_) {
      return 0;
    }
    return current_ - source_.data();
  }

  std::string source_;
  const char* current_;
  const char* end_;
  diagnostic_handler& diag_;
  bool internal_;
  std::unordered_set<std::string> recursed_;

public:
  /// Create a new parser from `source`. The `internal` flag disables setting
  /// `location`.
  explicit parser(std::string source, diagnostic_handler& diag, bool internal,
                  std::unordered_set<std::string> recursed)
    : source_{std::move(source)},
      current_{source_.data()},
      end_{source_.data() + source_.size()},
      diag_{diag},
      internal_{internal},
      recursed_{std::move(recursed)} {
  }

  auto parse() -> std::optional<std::vector<located<operator_ptr>>> {
    try {
      (void)legacy_accept("#!" >> *(parsers::any - '\n'));
      return parse_pipeline();
    } catch (const diagnostic& diag) {
      diag_.emit(diag);
      return {};
    }
  }
};

} // namespace

auto make_parser_interface(std::string source, diagnostic_handler& diag)
  -> std::unique_ptr<parser_interface> {
  return std::make_unique<parser>(std::move(source), diag, true,
                                  std::unordered_set<std::string>{});
}

auto parse(std::string source, diagnostic_handler& diag)
  -> std::optional<std::vector<located<operator_ptr>>> {
  auto recursed = std::unordered_set<std::string>{};
  return parser{std::move(source), diag, false, recursed}.parse();
}

auto parse_internal(std::string source) -> caf::expected<pipeline> {
  auto [pipe, diags] = parse_internal_with_diags(std::move(source));
  if (not pipe) {
    return caf::make_error(
      ec::parse_error, fmt::format("could not parse pipeline: {::?}", diags));
  }
  return std::move(*pipe);
}

auto parse_internal_with_diags(std::string source)
  -> std::pair<std::optional<pipeline>, std::vector<diagnostic>> {
  auto diag = collecting_diagnostic_handler{};
  auto recursed = std::unordered_set<std::string>{};
  auto ops = parser{std::move(source), diag, true, recursed}.parse();
  return {ops ? to_pipeline(std::move(*ops)) : std::optional<pipeline>{},
          std::move(diag).collect()};
}

void set_operator_aliases(std::unordered_map<std::string, std::string> map) {
  auto& [mutex, x] = get_aliases_global();
  auto lock = std::unique_lock{mutex};
  x = std::move(map);
}

auto to_pipeline(std::vector<located<operator_ptr>> ops) -> pipeline {
  auto result = std::vector<operator_ptr>{};
  for (auto& op : ops) {
    result.push_back(std::move(op.inner));
  }
  return pipeline{std::move(result)};
}

} // namespace tenzir::tql
