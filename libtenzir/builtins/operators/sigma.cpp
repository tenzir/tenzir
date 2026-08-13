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
#include <tenzir/detail/flat_map.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/io/read.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/result.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/sigma.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/filter.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <arrow/record_batch.h>
#include <fmt/format.h>
#include <re2/re2.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <functional>
#include <ranges>
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

namespace ir = tenzir::sigma;

template <class T>
using ParseResult = Result<T, diagnostic>;

template <class... Ts>
auto parse_failure(fmt::format_string<Ts...> str, Ts&&... xs)
  -> Err<diagnostic> {
  return Err{diagnostic::error(str, std::forward<Ts>(xs)...).done()};
}

ParseResult<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt,
                       std::string_view key);

ParseResult<std::string>
validate_regex(std::string regex, std::string_view key);

using ExpressionMap = detail::flat_map<std::string, ast::expression>;

/// Helpers to combine lowered sub-expressions when resolving search
/// identifiers and quantified selectors from the Sigma IR.
namespace expression_algebra {

/// Joins a set of sub-expressions into a conjunction or disjunction.
template <ast::binary_op Op>
auto join(std::vector<ast::expression> xs) -> ast::expression {
  if (xs.empty()) {
    return ast::constant{false, location::unknown};
  }
  auto result = std::move(xs[0]);
  for (auto i = size_t{1}; i < xs.size(); ++i) {
    result = ast::binary_expr{std::move(result), Op, std::move(xs[i])};
  }
  return result;
}

auto flatten(ast::expression x, ast::binary_op op,
             std::vector<ast::expression>& result) -> void {
  if (auto* binary = try_as<ast::binary_expr>(x); binary and binary->op == op) {
    flatten(std::move(binary->left), op, result);
    flatten(std::move(binary->right), op, result);
    return;
  }
  result.push_back(std::move(x));
}

template <ast::binary_op Op>
auto force(ast::expression x) -> ast::expression {
  auto const from
    = Op == ast::binary_op::and_ ? ast::binary_op::or_ : ast::binary_op::and_;
  auto xs = std::vector<ast::expression>{};
  flatten(std::move(x), from, xs);
  return join<Op>(std::move(xs));
}

} // namespace expression_algebra

/// Transforms a string that may contain Sigma glob wildcards into a regular
/// expression with respective metacharacters. Sigma patterns are always
/// case-insensitive.
ParseResult<std::string>
transform_sigma_string(std::string_view str, std::string_view fmt,
                       std::string_view key) {
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
  auto result
    = fmt.empty()
        ? fmt::format("(?i:{})", rx)
        : fmt::format("(?i:{})", fmt::format(TENZIR_FMT_RUNTIME(fmt), rx));
  return validate_regex(std::move(result), key);
}

ParseResult<std::string>
validate_regex(std::string regex, std::string_view key) {
  auto compiled = re2::RE2{regex, re2::RE2::CannedOptions::Quiet};
  if (not compiled.ok()) {
    return Err{
      diagnostic::error("invalid regular expression for Sigma field `{}`", key)
        .note("regex: {}", regex)
        .note("error: {}", compiled.error())
        .done()};
  }
  return regex;
}

auto make_field_expr(std::string_view name) -> ast::expression {
  auto parts = detail::split(name, ".");
  TENZIR_ASSERT(not parts.empty());
  auto result = ast::expression{ast::root_field{
    ast::identifier{std::string{parts[0]}, location::unknown}, true}};
  for (auto part : parts | std::views::drop(1)) {
    result = ast::field_access{std::move(result), location::unknown, true,
                               ast::identifier{std::string{part},
                                               location::unknown}};
  }
  return result;
}

auto make_constant(data const& value) -> ast::expression {
  return match(
    value,
    [](pattern const&) -> ast::expression {
      TENZIR_UNREACHABLE();
    },
    []<class T>(T const& x) -> ast::expression
      requires(not std::same_as<T, pattern>)
    {
      return ast::constant{x, location::unknown};
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
  return ast::binary_expr{std::move(left), op, std::move(right)};
}

// -- lowering: Sigma IR -> TQL expressions -------------------------------

/// Lowers one detection item (`field|modifiers: value(s)`) into a TQL
/// expression. The IR has already validated the modifier chain and value
/// types; this function only implements the executable semantics.
auto lower_item(ir::DetectionItem const& item) -> ParseResult<ast::expression> {
  auto const& field = item.field.raw;
  auto key = field;
  for (auto const& modifier : item.modifiers) {
    key += '|';
    key += modifier;
  }
  auto op = ast::binary_op::eq;
  auto all = false;
  auto anchor_regex = true;
  auto transform_regex = Option<std::string>{};
  auto raw_regex = false;
  auto stringify_for_regex = false;
  auto contains = false;
  auto transforms
    = std::vector<std::function<ParseResult<data>(data const&)>>{};
  for (auto const& modifier : item.modifiers) {
    if (modifier == "all") {
      all = true;
    } else if (modifier == "lt") {
      op = ast::binary_op::lt;
    } else if (modifier == "lte") {
      op = ast::binary_op::leq;
    } else if (modifier == "gt") {
      op = ast::binary_op::gt;
    } else if (modifier == "gte") {
      op = ast::binary_op::geq;
    } else if (modifier == "contains") {
      anchor_regex = false;
      transform_regex = ".*{}.*";
      contains = true;
    } else if (modifier == "base64") {
      auto encode = [](data const& x) -> ParseResult<data> {
        if (auto const* str = try_as<std::string>(&x)) {
          return detail::base64::encode(*str);
        }
        return parse_failure("Sigma modifier `base64` only works with strings");
      };
      transforms.emplace_back(encode);
    } else if (modifier == "base64offset") {
      auto encode = [](data const& x) -> ParseResult<data> {
        auto const* str = try_as<std::string>(&x);
        if (not str) {
          return parse_failure(
            "Sigma modifier `base64offset` only works with strings");
        }
        static constexpr auto start = std::array<size_t, 3>{0, 2, 3};
        static constexpr auto end = std::array<size_t, 3>{0, 3, 2};
        auto xs = std::vector<std::string>(3);
        for (auto i = size_t{0}; i < 3; ++i) {
          auto padded = std::string(i, ' ') + *str;
          auto b64 = detail::base64::encode(padded);
          auto len = b64.size() - end[(str->size() + i) % 3];
          xs[i] = b64.substr(start[i], len - start[i]);
        }
        return list{xs[0], xs[1], xs[2]};
      };
      transforms.emplace_back(encode);
    } else if (modifier == "startswith") {
      anchor_regex = false;
      transform_regex = "^{}.*";
      stringify_for_regex = true;
    } else if (modifier == "endswith") {
      anchor_regex = false;
      transform_regex = ".*{}$";
      stringify_for_regex = true;
    } else if (modifier == "re") {
      anchor_regex = false;
      raw_regex = true;
    } else if (modifier == "cidr") {
      op = ast::binary_op::in;
    } else {
      // The IR validates modifier chains before lowering.
      return parse_failure("Sigma modifier `{}` is not yet implemented",
                           modifier);
    }
  }
  auto modify = [&](data const& x) -> ParseResult<data> {
    auto result = x;
    for (auto const& transform : transforms) {
      TRY(auto y, transform(result));
      result = std::move(y);
    }
    return result;
  };
  auto make_predicate_expr
    = [&](data const& value) -> ParseResult<ast::expression> {
    auto make_string_predicate
      = [&](std::string str) -> ParseResult<ast::expression> {
      auto format = transform_regex.unwrap_or(anchor_regex ? "^{}$" : "{}");
      if (raw_regex) {
        auto regex = fmt::format(TENZIR_FMT_RUNTIME(format), std::move(str));
        TRY(auto valid, validate_regex(std::move(regex), key));
        return make_regex_expr(make_field_expr(field), std::move(valid));
      }
      TRY(auto pattern, transform_sigma_string(str, format, key));
      return make_regex_expr(make_field_expr(field), std::move(pattern));
    };
    if (auto const* str = try_as<std::string>(&value)) {
      return make_string_predicate(*str);
    }
    if (auto const* values = try_as<list>(&value)) {
      // Only the `base64offset` transform produces lists here.
      TENZIR_ASSERT(values->size() == 3);
      auto disjuncts = std::vector<ast::expression>{};
      for (auto const& x : *values) {
        if (auto const* str = try_as<std::string>(&x);
            str and transform_regex) {
          if (raw_regex) {
            auto regex
              = fmt::format(TENZIR_FMT_RUNTIME(*transform_regex), *str);
            TRY(auto valid, validate_regex(std::move(regex), key));
            disjuncts.emplace_back(
              make_regex_expr(make_field_expr(field), std::move(valid)));
          } else {
            TRY(auto pattern,
                transform_sigma_string(*str, *transform_regex, key));
            disjuncts.emplace_back(
              make_regex_expr(make_field_expr(field), std::move(pattern)));
          }
        } else if (raw_regex) {
          auto regex = to_string(x);
          TRY(auto valid, validate_regex(std::move(regex), key));
          disjuncts.emplace_back(
            make_regex_expr(make_field_expr(field), std::move(valid)));
        } else {
          auto binary_op = contains and is<subnet>(x) ? ast::binary_op::in : op;
          disjuncts.emplace_back(make_binary_expr(make_field_expr(field),
                                                  binary_op, make_constant(x)));
        }
      }
      return expression_algebra::join<ast::binary_op::or_>(
        std::move(disjuncts));
    }
    if (stringify_for_regex or raw_regex) {
      return make_string_predicate(to_string(value));
    }
    if (contains and is<subnet>(value)) {
      return make_binary_expr(make_field_expr(field), ast::binary_op::in,
                              make_constant(value));
    }
    return make_binary_expr(make_field_expr(field), op, make_constant(value));
  };
  if (item.value_is_list) {
    auto connective = std::vector<ast::expression>{};
    for (auto const& value : item.values) {
      TRY(auto x, modify(value));
      TRY(auto expr, make_predicate_expr(x));
      connective.emplace_back(std::move(expr));
    }
    return all ? expression_algebra::join<ast::binary_op::and_>(
                   std::move(connective))
               : expression_algebra::join<ast::binary_op::or_>(
                   std::move(connective));
  }
  TENZIR_ASSERT(item.values.size() == 1);
  TRY(auto x, modify(item.values[0]));
  return make_predicate_expr(x);
}

/// Lowers a named detection: items within a group are AND-linked, groups are
/// OR-linked (the YAML list-of-maps form).
auto lower_detection(ir::Detection const& detection)
  -> ParseResult<ast::expression> {
  auto disjuncts = std::vector<ast::expression>{};
  for (auto const& group : detection.groups) {
    auto conjuncts = std::vector<ast::expression>{};
    for (auto const& item : group) {
      TRY(auto expression, lower_item(item));
      conjuncts.emplace_back(std::move(expression));
    }
    disjuncts.emplace_back(
      expression_algebra::join<ast::binary_op::and_>(std::move(conjuncts)));
  }
  return expression_algebra::join<ast::binary_op::or_>(std::move(disjuncts));
}

/// Resolves all search identifiers matching a wildcard pattern, in the
/// deterministic order of the expression map.
auto search(ExpressionMap const& expressions, std::string_view pattern)
  -> std::vector<ast::expression> {
  auto result = std::vector<ast::expression>{};
  for (auto const& [name, expression] : expressions) {
    if (ir::wildcard_match(pattern, name)) {
      result.push_back(expression);
    }
  }
  return result;
}

/// Lowers a parsed condition tree by substituting search identifiers.
auto lower_condition(ir::Condition const& condition,
                     ExpressionMap const& expressions)
  -> ParseResult<ast::expression> {
  return match(
    condition.node,
    [&](ir::Identifier const& x) -> ParseResult<ast::expression> {
      if (auto i = expressions.find(x.name); i != expressions.end()) {
        return i->second;
      }
      // A bare wildcard pattern AND-links all matching identifiers.
      return expression_algebra::join<ast::binary_op::and_>(
        search(expressions, x.name));
    },
    [&](ir::Quantified const& x) -> ParseResult<ast::expression> {
      auto pattern = x.all_identifiers ? std::string_view{"*"} : x.pattern;
      if (not x.all_identifiers) {
        if (auto i = expressions.find(x.pattern); i != expressions.end()) {
          // Quantification over one identifier re-links its internal
          // structure. (Known v2.1 deviation; corrected in a follow-up.)
          return x.quantifier == ir::Quantifier::all
                   ? expression_algebra::force<ast::binary_op::and_>(i->second)
                   : expression_algebra::force<ast::binary_op::or_>(i->second);
        }
      }
      auto matches = search(expressions, pattern);
      return x.quantifier == ir::Quantifier::all
               ? expression_algebra::join<ast::binary_op::and_>(
                   std::move(matches))
               : expression_algebra::join<ast::binary_op::or_>(
                   std::move(matches));
    },
    [&](ir::Negation const& x) -> ParseResult<ast::expression> {
      TRY(auto operand, lower_condition(*x.operand, expressions));
      return ast::expression{
        ast::unary_expr{{ast::unary_op::not_, {}}, std::move(operand)}};
    },
    [&](ir::Conjunction const& x) -> ParseResult<ast::expression> {
      TRY(auto left, lower_condition(*x.left, expressions));
      TRY(auto right, lower_condition(*x.right, expressions));
      return make_binary_expr(std::move(left), ast::binary_op::and_,
                              std::move(right));
    },
    [&](ir::Disjunction const& x) -> ParseResult<ast::expression> {
      TRY(auto left, lower_condition(*x.left, expressions));
      TRY(auto right, lower_condition(*x.right, expressions));
      return make_binary_expr(std::move(left), ast::binary_op::or_,
                              std::move(right));
    });
}

/// Compiles one YAML document through the typed Sigma IR into an executable
/// expression: parse and validate, lower named detections, then substitute
/// them into the condition.
auto compile_rule(data const& yaml) -> ParseResult<ast::expression> {
  TRY(auto document, ir::parse_document(yaml));
  auto const* rule = try_as<ir::DetectionRule>(document.content);
  TENZIR_ASSERT(rule); // unsupported kinds fail in `parse_document`
  auto expressions = ExpressionMap{};
  for (auto const& [name, detection] : rule->detections) {
    TRY(auto expression, lower_detection(detection));
    expressions[name] = std::move(expression);
  }
  return lower_condition(rule->condition, expressions);
}

struct RuleEntry {
  data yaml;
  ast::expression rule;
};

using RuleMap = std::unordered_map<std::string, RuleEntry>;

auto emit_ignored_rule(const std::filesystem::path& path, diagnostic reason,
                       diagnostic_handler& dh) -> void {
  auto builder
    = diagnostic::warning("sigma operator ignores rule '{}'", path.string())
        .note("{}", reason.message);
  for (const auto& note : reason.notes) {
    switch (note.kind) {
      case diagnostic_note_kind::note:
        builder = std::move(builder).note("{}", note.message);
        break;
      case diagnostic_note_kind::usage:
        builder = std::move(builder).usage("{}", note.message);
        break;
      case diagnostic_note_kind::hint:
        builder = std::move(builder).hint("{}", note.message);
        break;
      case diagnostic_note_kind::docs:
        builder = std::move(builder).docs("{}", note.message);
        break;
    }
  }
  std::move(builder).emit(dh);
}

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
  auto parsed_rule = compile_rule(*yaml);
  if (parsed_rule.is_err()) {
    emit_ignored_rule(path, std::move(parsed_rule).unwrap_err(), dh);
    return;
  }
  auto rule = std::move(parsed_rule).unwrap();
  auto provider = session_provider::make(dh);
  if (not resolve_entities(rule, provider.as_session())) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to resolve sigma rule")
      .emit(dh);
    return;
  }
  rules[path.string()] = {std::move(*yaml), std::move(rule)};
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
  -> Option<table_slice> {
  auto event = filter2(input, rule, dh, false);
  if (event.rows() == 0) {
    return None{};
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
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto refresh_interval = Option<located<duration>>{};
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
    d.parallelizable();
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
};

} // namespace

} // namespace tenzir::plugins::sigma

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::plugin)
