//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/data.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/io/read.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/filter.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <ranges>
#include <regex>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir::plugins::sigma {

// TODO: A lot of code in here is directly copied from
// src/concept/parseable/expression.cpp. We should factor the implementation in
// the future.

namespace {

caf::expected<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt);

using expression_map = std::map<std::string, ast::expression>;

/// A symbol-table-like parser for Sigma search identifers. In addition to the
/// exact match as in a symbol table, this parser also performs the additional
/// syntax "1/all of X" where X can be "them", a search identifier, or a
/// wildcard pattern. This parsers is effective a predicate operand in the
/// "condition" field of the "detection" attribute.
struct search_id_symbol_table : parser_base<search_id_symbol_table> {
  using attribute = ast::expression;

  enum class quantifier { all, any };

  /// Constructs a search ID symbol table from an expression map.
  explicit search_id_symbol_table(const expression_map& exprs) {
    id.symbols.reserve(exprs.size());
    for (auto& [key, value] : exprs) {
      id.symbols.emplace(key, value);
    }
  }

  /// Joins a set of sub-expressions into a conjunction or disjunction.
  template <ast::binary_op Op>
  static ast::expression join(std::vector<ast::expression> xs) {
    TENZIR_ASSERT(not xs.empty());
    auto result = std::move(xs[0]);
    for (auto i = size_t{1}; i < xs.size(); ++i) {
      result = ast::binary_expr{std::move(result), {Op, {}}, std::move(xs[i])};
    }
    return result;
  }

  template <ast::binary_op Op>
  static ast::expression force(ast::expression x) {
    return x;
  }

  /// Performs *-wildcard search on all search identifiers.
  [[nodiscard]] std::vector<ast::expression> search(std::string str) const {
    auto rx_str = std::regex_replace(str, std::regex("\\*"), ".*");
    auto rx = std::regex{rx_str};
    std::vector<ast::expression> result;
    for (auto& [sym, expr] : id.symbols) {
      if (std::regex_search(sym.begin(), sym.end(), rx)) {
        result.push_back(expr);
      }
    }
    return result;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& result) const {
    using namespace parser_literals;
    // clang-format off
    auto ws = ignore(*parsers::space);
    auto pattern = +(parsers::any - parsers::space);
    auto selection
      = "them"_p ->* [this] { return search("*"); }
      | pattern ->* [this](std::string str) { return search(std::move(str)); }
      ;
    auto expr
      = "all of"_p >> ws >> id ->* force<ast::binary_op::and_>
      | "1 of"_p >> ws >> id ->* force<ast::binary_op::or_>
      | "all of"_p >> ws >> selection ->* join<ast::binary_op::and_>
      | "1 of"_p >> ws >> selection ->* join<ast::binary_op::or_>
      | id
      | selection ->* join<ast::binary_op::and_>
      ;
    // clang-format on
    return expr(f, l, result);
  }

  symbol_table<ast::expression> id;
};

/// Parses the "detection" attribute from a Sigma rule. See the Sigma wiki for
/// details: https://github.com/Neo23x0/sigma/wiki/Specification#detection
struct detection_parser : parser_base<detection_parser> {
  using attribute = ast::expression;

  explicit detection_parser(const expression_map& exprs) : search_id{exprs} {
  }

  static ast::expression
  to_expr(std::tuple<ast::expression,
                     std::vector<std::tuple<ast::binary_op, ast::expression>>>
            expr) {
    auto& [x, xs] = expr;
    auto result = std::move(x);
    for (auto& [op, expr] : xs) {
      result = ast::binary_expr{std::move(result), {op, {}}, std::move(expr)};
    }
    return result;
  };

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, ast::expression& result) const {
    using namespace parser_literals;
    auto ws = ignore(*parsers::space);
    auto negate = [](ast::expression x) {
      return ast::unary_expr{{ast::unary_op::not_, {}}, std::move(x)};
    };
    rule<Iterator, ast::expression> expr;
    rule<Iterator, ast::expression> group;
    // clang-format off
    group
      = '(' >> ws >> ref(expr) >> ws >> ')'
      | "not"_p >> ws >> '(' >> ws >> (ref(expr) ->* negate) >> ws >> ')'
      | "not"_p >> ws >> search_id ->* negate
      | search_id
      ;
    auto and_or
      = "or"_p  ->* [] { return ast::binary_op::or_; }
      | "and"_p  ->* [] { return ast::binary_op::and_; }
      ;
    auto tail = (ws >> and_or >> ws >> ref(group))
      ->* [](std::tuple<ast::binary_op, ast::expression> x) { return x; };
    expr
      = (group >> *tail >> ws) ->* to_expr
      ;
    // clang-format on
    auto p = expr >> parsers::eoi;
    return p(f, l, result);
  }

  search_id_symbol_table search_id;
};

/// Transforms a string that may contain Sigma glob wildcards into a regular
/// expression with respective metacharacters. Sigma patterns are always
/// case-insensitive.
caf::expected<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt) {
  // The following invariants apply according to the Sigma spec:
  // - All values are treated as case-insensitive strings
  // - You can use wildcard characters '*' and '?' in strings
  // - Wildcards can be escaped with \, e.g. \*. If some wildcard after a
  //   backslash should be searched, the backslash has to be escaped: \\*.
  // - Regular expressions are case-sensitive by default
  // - You don't have to escape characters except the string quotation
  //   marks '
  auto f = str.begin();
  auto l = str.end();
  std::string rx;
  // FIXME: this is a pretty hand-wavy approach to transforming a glob string
  // to a valid regex. We need to revisit this once we have actual pattern
  // support in the query language.
  while (f != l) {
    const auto c = *f++;
    switch (c) {
      case '*':
        rx += ".*";
        break;
      case '?':
        rx += '.';
        break;
      case '.':
      case '[':
      case ']':
      case '(':
      case ')':
      case '{':
      case '}':
      case '^':
      case '$':
        rx += '\\';
        rx += c;
        break;
      case '\\':
        if (f != l and (*f == '?' or *f == '*' or *f == '\\')) {
          // Edge-case: The user intended to escape the glob character.
          rx += '\\';
          rx += *f++;
          break;
        }
        rx += "\\\\";
        break;
      default:
        rx += c;
        break;
    }
  }
  if (fmt.empty()) {
    return fmt::format("(?i:{})", rx);
  }
  return fmt::format("(?i:{})", fmt::format(TENZIR_FMT_RUNTIME(fmt), rx));
}

auto make_field_expr(std::string_view name) -> ast::expression {
  auto parts = detail::split(name, ".");
  TENZIR_ASSERT(not parts.empty());
  auto result = ast::expression{ast::root_field{
    ast::identifier{std::string{parts[0]}, location::unknown}, false}};
  for (auto part : parts | std::views::drop(1)) {
    result = ast::field_access{std::move(result), location::unknown, false,
                               ast::identifier{std::string{part},
                                               location::unknown}};
  }
  return result;
}

auto make_constant(data const& value) -> ast::expression {
  return match(value, detail::overload{
                        [](pattern const&) -> ast::expression {
                          TENZIR_UNREACHABLE();
                        },
                        []<class T>(T const& x) -> ast::expression
                          requires(not std::same_as<T, pattern>)
                        {
                          return ast::constant{x, location::unknown};
                        },
                        });
}

auto make_regex_expr(ast::expression field, std::string regex)
  -> ast::expression {
  return ast::function_call{
    ast::entity{{ast::identifier{"match_regex", location::unknown}}},
    {std::move(field), ast::constant{std::move(regex), location::unknown}},
    location::unknown,
    false};
}

auto make_binary_expr(ast::expression left, ast::binary_op op,
                      ast::expression right) -> ast::expression {
  return ast::binary_expr{
    std::move(left), {op, location::unknown}, std::move(right)};
}

} // namespace

caf::expected<ast::expression> parse_search_id(const data& yaml) {
  if (auto xs = try_as<record>(&yaml)) {
    auto result = std::vector<ast::expression>{};
    for (auto& [key, rhs] : *xs) {
      auto keys = detail::split(key, "|");
      auto field = std::string{keys[0]};
      auto op = ast::binary_op::eq;
      auto all = false;
      auto anchor_regex = true;
      auto transform_regex = std::optional<std::string>{};
      std::vector<std::function<caf::expected<data>(const data&)>> transforms;
      for (auto i = keys.begin() + 1; i != keys.end(); ++i) {
        if (*i == "all") {
          all = true;
        } else if (*i == "lt") {
          op = ast::binary_op::lt;
        } else if (*i == "lte") {
          op = ast::binary_op::leq;
        } else if (*i == "gt") {
          op = ast::binary_op::gt;
        } else if (*i == "gte") {
          op = ast::binary_op::geq;
        } else if (*i == "contains") {
          anchor_regex = false;
          transform_regex = ".*{}.*";
        } else if (*i == "base64") {
          auto encode = [](const data& x) -> caf::expected<data> {
            if (const auto* str = try_as<std::string>(&x)) {
              return detail::base64::encode(*str);
            }
            return caf::make_error(ec::type_clash,
                                   "base64 only works with strings");
          };
          transforms.emplace_back(encode);
        } else if (*i == "base64offset") {
          auto encode = [](const data& x) -> caf::expected<data> {
            const auto* str = try_as<std::string>(&x);
            if (not str) {
              return caf::make_error(ec::type_clash,
                                     "base64offset only works with strings");
            }
            static constexpr std::array<size_t, 3> start = {{0, 2, 3}};
            static constexpr std::array<size_t, 3> end = {{0, 3, 2}};
            std::vector<std::string> xs(3);
            for (size_t i = 0; i < 3; ++i) {
              auto padded = std::string(i, ' ') + *str;
              auto b64 = detail::base64::encode(padded);
              auto len = b64.size() - end[(str->size() + i) % 3];
              xs[i] = b64.substr(start[i], len - start[i]);
            }
            return list{xs[0], xs[1], xs[2]};
          };
          transforms.emplace_back(encode);
        } else if (*i == "utf16le" or *i == "wide") {
          return caf::make_error(ec::unimplemented,
                                 "utf16le/wide not yet implemented");
        } else if (*i == "utf16be") {
          return caf::make_error(ec::unimplemented,
                                 "utf16be not yet implemented");
        } else if (*i == "utf16") {
          return caf::make_error(ec::unimplemented,
                                 "utf16 not yet implemented");
        } else if (*i == "startswith") {
          anchor_regex = false;
          transform_regex = "^{}.*";
        } else if (*i == "endswith") {
          anchor_regex = false;
          transform_regex = ".*{}$";
        } else if (*i == "re") {
          anchor_regex = false;
        } else if (*i == "cidr") {
          op = ast::binary_op::in;
        } else if (*i == "expand") {
          return caf::make_error(ec::unimplemented,
                                 "expand modifier not yet implemented");
        }
      }
      auto modify = [&](const data& x) -> caf::expected<data> {
        auto result = x;
        for (const auto& f : transforms) {
          if (auto y = f(result)) {
            result = std::move(*y);
          } else {
            return y.error();
          }
        }
        return result;
      };
      auto make_predicate_expr = [&](const data& value) -> ast::expression {
        if (auto str = try_as<std::string>(&value)) {
          auto fmt = transform_regex.value_or(anchor_regex ? "^{}$" : "{}");
          if (auto pat = transform_sigma_string(*str, fmt)) {
            return make_regex_expr(make_field_expr(field), std::move(*pat));
          }
        }
        if (auto values = try_as<list>(&value)) {
          TENZIR_ASSERT(values->size() == 3);
          auto disjuncts = std::vector<ast::expression>{};
          for (const auto& x : *values) {
            if (auto str = try_as<std::string>(&x); str and transform_regex) {
              if (auto pat = transform_sigma_string(*str, *transform_regex)) {
                disjuncts.emplace_back(
                  make_regex_expr(make_field_expr(field), std::move(*pat)));
              }
            } else {
              disjuncts.emplace_back(
                make_binary_expr(make_field_expr(field), op, make_constant(x)));
            }
          }
          return search_id_symbol_table::join<ast::binary_op::or_>(
            std::move(disjuncts));
        }
        return make_binary_expr(make_field_expr(field), op,
                                make_constant(value));
      };
      if (is<record>(rhs)) {
        return caf::make_error(ec::type_clash, "nested records not allowed");
      }
      if (auto values = try_as<list>(&rhs)) {
        auto connective = std::vector<ast::expression>{};
        for (const auto& value : *values) {
          if (is<list>(value)) {
            return caf::make_error(ec::type_clash, "nested lists disallowed");
          }
          if (is<record>(value)) {
            return caf::make_error(ec::type_clash, "nested records disallowed");
          }
          if (auto x = modify(value)) {
            connective.emplace_back(make_predicate_expr(*x));
          } else {
            return x.error();
          }
        }
        result.emplace_back(
          all ? search_id_symbol_table::join<ast::binary_op::and_>(
                  std::move(connective))
              : search_id_symbol_table::join<ast::binary_op::or_>(
                  std::move(connective)));
      } else {
        if (auto x = modify(rhs)) {
          result.emplace_back(make_predicate_expr(*x));
        } else {
          return x.error();
        }
      }
    }
    return search_id_symbol_table::join<ast::binary_op::and_>(
      std::move(result));
  } else if (auto xs = try_as<list>(&yaml)) {
    auto result = std::vector<ast::expression>{};
    for (auto& search_id : *xs) {
      if (auto expr = parse_search_id(search_id)) {
        result.push_back(std::move(*expr));
      } else {
        return expr.error();
      }
    }
    return search_id_symbol_table::join<ast::binary_op::or_>(std::move(result));
  } else {
    return caf::make_error(
      ec::type_clash, fmt::format("search id '{}' not a list or record", yaml));
  }
}

caf::expected<ast::expression> parse_rule(const data& yaml) {
  auto xs = try_as<record>(&yaml);
  if (not xs) {
    return caf::make_error(ec::type_clash, "rule must be a record");
  }
  // Extract detection attribute.
  const record* detection;
  if (auto i = xs->find("detection"); i == xs->end()) {
    return caf::make_error(ec::invalid_query, "no detection attribute");
  } else {
    detection = try_as<record>(&i->second);
  }
  if (not detection) {
    return caf::make_error(ec::type_clash, "detection not a record");
  }
  // Resolve all named sub-expression except for "condition".
  expression_map exprs;
  for (auto& [key, value] : *detection) {
    if (key == "condition") {
      continue;
    }
    if (auto expr = parse_search_id(value)) {
      exprs[key] = std::move(*expr);
    } else {
      return expr.error();
    }
  }
  // Extract condition.
  const std::string* condition;
  if (auto i = detection->find("condition"); i == detection->end()) {
    return caf::make_error(ec::invalid_query, "no condition key");
  } else {
    condition = try_as<std::string>(&i->second);
  }
  if (not condition) {
    return caf::make_error(ec::type_clash, "condition not a string");
  }
  // Parse condition.
  ast::expression result;
  detection_parser p{exprs};
  if (not p(*condition, result)) {
    return caf::make_error(ec::parse_error, "invalid condition syntax");
  }
  return result;
}

namespace {

struct RuleEntry {
  data yaml;
  ast::expression rule;
};

using RuleMap = std::unordered_map<std::string, RuleEntry>;

auto load_rules(const std::filesystem::path& path, RuleMap& rules,
                diagnostic_handler& dh) -> void {
  if (std::filesystem::is_directory(path)) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      load_rules(entry.path(), rules, dh);
    }
    return;
  }
  if (path.extension() != ".yml" and path.extension() != ".yaml") {
    // We silently ignore non-yaml files.
    return;
  }
  auto query = tenzir::io::read(path);
  if (not query) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to read file: {}", query.error())
      .emit(dh);
    return;
  }
  auto query_str = std::string_view{
    reinterpret_cast<const char*>(query->data()),
    reinterpret_cast<const char*>(query->data() + query->size())}; // NOLINT
  auto yaml = from_yaml(query_str);
  if (not yaml) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to parse yaml: {}", yaml.error())
      .emit(dh);
    return;
  }
  if (not is<record>(*yaml)) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("rule is not a YAML dictionary")
      .emit(dh);
    return;
  }
  auto rule = parse_rule(*yaml);
  if (not rule) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to parse sigma rule: {}", rule.error())
      .emit(dh);
    return;
  }
  auto provider = session_provider::make(dh);
  if (not resolve_entities(*rule, provider.as_session())) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to resolve sigma rule")
      .emit(dh);
    return;
  }
  rules[path.string()] = {std::move(*yaml), std::move(*rule)};
}

auto update_rules(const std::filesystem::path& path, RuleMap& rules,
                  diagnostic_handler& dh) -> void {
  auto old_rules = std::exchange(rules, {});
  load_rules(path, rules, dh);
  for (const auto& [rule_path, rule] : rules) {
    const auto old_rule = old_rules.find(rule_path);
    if (old_rule == old_rules.end()) {
      TENZIR_VERBOSE("added Sigma rule {}", rule_path);
    } else if (old_rule->second.yaml != rule.yaml) {
      TENZIR_VERBOSE("updated Sigma rule {}", rule_path);
    }
  }
  for (const auto& [rule_path, _] : old_rules) {
    if (not rules.contains(rule_path)) {
      TENZIR_VERBOSE("removed Sigma rule {}", rule_path);
    }
  }
}

auto make_sigma_slice(const table_slice& input, const data& yaml,
                      const ast::expression& rule, diagnostic_handler& dh)
  -> std::optional<table_slice> {
  auto event = filter2(input, rule, dh, false);
  if (event.rows() == 0) {
    return std::nullopt;
  }
  auto [event_schema, event_array] = offset{}.get(event);
  auto [rule_schema, rule_array] = [&] {
    auto rule_builder = series_builder{};
    for (auto i = size_t{0}; i < event.rows(); ++i) {
      rule_builder.data(yaml);
    }
    return rule_builder.finish_assert_one_array();
  }();
  const auto result_schema = type{
    "tenzir.sigma",
    record_type{
      {"event", event_schema},
      {"rule", rule_schema},
    },
  };
  auto batch
    = arrow::RecordBatch::Make(result_schema.to_arrow_schema(),
                               detail::narrow<int64_t>(event.rows()),
                               {std::move(event_array), std::move(rule_array)});
  return table_slice{batch, result_schema};
}

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  explicit sigma_operator(duration refresh_interval, std::string path)
    : refresh_interval_{refresh_interval}, path_{std::move(path)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = RuleMap{};
    auto path = std::filesystem::path{path_};
    update_rules(path, rules, ctrl.diagnostics());
    auto last_update = std::chrono::steady_clock::now();
    co_yield {}; // signal that we're done initializing
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      if (now - last_update > refresh_interval_) {
        update_rules(path, rules, ctrl.diagnostics());
        last_update = now;
      }
      for (const auto& [_, entry] : rules) {
        if (auto result = make_sigma_slice(slice, entry.yaml, entry.rule,
                                           ctrl.diagnostics())) {
          co_yield std::move(*result);
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "sigma";
  }

  auto location() const -> operator_location override {
    // The operator is referring to files, and the user likely assumes that to
    // be relative to the current process, so we default to local here.
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sigma_operator& x) -> bool {
    return f.object(x)
      .pretty_name("sigma_operator")
      .fields(f.field("refresh_interval", x.refresh_interval_),
              f.field("path", x.path_));
  }

private:
  duration refresh_interval_ = {};
  std::string path_ = {};
};

struct SigmaArgs {
  std::string path;
  duration refresh_interval = std::chrono::seconds{5};
};

class Sigma final : public Operator<table_slice, table_slice> {
public:
  explicit Sigma(SigmaArgs args) : args_{std::move(args)}, path_{args_.path} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    update_rules(path_, rules_, ctx.dh());
    last_update_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto now = std::chrono::steady_clock::now();
    if (now - last_update_ > args_.refresh_interval) {
      update_rules(path_, rules_, ctx.dh());
      last_update_ = now;
    }
    for (const auto& [_, entry] : rules_) {
      if (auto result
          = make_sigma_slice(input, entry.yaml, entry.rule, ctx.dh())) {
        co_await push(std::move(*result));
      }
    }
  }

private:
  SigmaArgs args_;
  std::filesystem::path path_;
  RuleMap rules_;
  // Rules are reloaded from disk in `start()`, and `last_update_` uses
  // `steady_clock`, so the default no-op snapshot behavior is sufficient.
  std::chrono::steady_clock::time_point last_update_ = {};
};

class plugin final : public virtual operator_plugin<sigma_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto refresh_interval = std::optional<located<duration>>{};
    auto path = std::string{};
    argument_parser2::operator_("sigma")
      .positional("path", path)
      .named("refresh_interval", refresh_interval)
      .parse(inv, ctx)
      .ignore();
    auto interval
      = refresh_interval ? refresh_interval->inner : std::chrono::seconds{5};
    if (refresh_interval and interval <= duration::zero()) {
      diagnostic::error("`refresh_interval` must be a positive duration")
        .primary(refresh_interval.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<sigma_operator>(interval, std::move(path));
  }

  auto describe() const -> Description override {
    auto d = Describer<SigmaArgs, Sigma>{};
    d.positional("path", &SigmaArgs::path);
    auto refresh_interval
      = d.named_optional("refresh_interval", &SigmaArgs::refresh_interval);
    d.validate([refresh_interval](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(refresh_interval);
          value and *value <= duration::zero()) {
        diagnostic::error("`refresh_interval` must be a positive duration")
          .primary(ctx.get_location(refresh_interval).value())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"sigma", "https://docs.tenzir.com/"
                                           "reference/operators"};
    auto refresh_interval = duration{std::chrono::seconds{5}};
    auto refresh_interval_arg = std::optional<located<std::string>>{};
    auto path = std::string{};
    parser.add("--refresh-interval", refresh_interval_arg,
               "<refresh-interval>");
    parser.add(path, "<rule-or-directory>");
    parser.parse(p);
    if (refresh_interval_arg) {
      if (not parsers::duration(refresh_interval_arg->inner,
                                refresh_interval)) {
        diagnostic::error("refresh interval is not a valid duration")
          .primary(refresh_interval_arg->source)
          .throw_();
      }
      if (refresh_interval <= duration::zero()) {
        diagnostic::error("`refresh_interval` must be a positive duration")
          .primary(refresh_interval_arg->source)
          .throw_();
      }
    }
    return std::make_unique<sigma_operator>(refresh_interval, std::move(path));
  }
};

} // namespace

} // namespace tenzir::plugins::sigma

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::plugin)
