//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "sigma/parse.hpp"

#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/string.hpp>
#include <tenzir/concept/printable/tenzir/data.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/error.hpp>
#include <tenzir/expression_visitors.hpp>

#include <array>
#include <map>
#include <regex>
#include <string>
#include <tuple>
#include <vector>

namespace tenzir::plugins::sigma {

// TODO: A lot of code in here is directly copied from
// src/concept/parseable/expression.cpp. We should factor the implementation in
// the future.

namespace {

caf::expected<pattern>
transform_sigma_string(std::string_view str, std::string_view fmt);

using expression_map = std::map<std::string, expression>;

/// A symbol-table-like parser for Sigma search identifers. In addition to the
/// exact match as in a symbol table, this parser also performs the additional
/// syntax "1/all of X" where X can be "them", a search identifier, or a
/// wildcard pattern. This parsers is effective a predicate operand in the
/// "condition" field of the "detection" attribute.
struct search_id_symbol_table : parser_base<search_id_symbol_table> {
  using attribute = expression;

  enum class quantifier { all, any };

  /// Constructs a search ID symbol table from an expression map.
  explicit search_id_symbol_table(const expression_map& exprs) {
    id.symbols.reserve(exprs.size());
    for (auto& [key, value] : exprs)
      id.symbols.emplace(key, value);
  }

  /// Joins a set of sub-expressions into a conjunction or disjunction.
  template <class Connective>
  static expression join(std::vector<expression> xs) {
    Connective result;
    result.reserve(xs.size());
    std::move(xs.begin(), xs.end(), std::back_insert_iterator(result));
    return hoist(expression{std::move(result)});
  }

  // Forces a conjunction or disjunction on a given expression.
  template <class Connective>
  static expression force(expression x) {
    auto transform = [](auto&& connective) {
      auto xs = static_cast<std::vector<expression>&>(connective);
      return expression{Connective{std::move(xs)}};
    };
    if constexpr (std::is_same_v<Connective, conjunction>)
      if (auto xs = try_as<disjunction>(&x)) {
        return transform(std::move(*xs));
      }
    if constexpr (std::is_same_v<Connective, disjunction>)
      if (auto xs = try_as<conjunction>(&x)) {
        return transform(std::move(*xs));
      }
    return x;
  }

  /// Performs *-wildcard search on all search identifiers.
  [[nodiscard]] std::vector<expression> search(std::string str) const {
    auto rx_str = std::regex_replace(str, std::regex("\\*"), ".*");
    auto rx = std::regex{rx_str};
    std::vector<expression> result;
    for (auto& [sym, expr] : id.symbols)
      if (std::regex_search(sym.begin(), sym.end(), rx))
        result.push_back(expr);
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
      | pattern ->* [this](std::string str) { return search(transform_sigma_string(str, {})->string()); }
      ;
    auto expr
      = "all of"_p >> ws >> id ->* force<conjunction>
      | "1 of"_p >> ws >> id ->* force<disjunction>
      | "all of"_p >> ws >> selection ->* join<conjunction>
      | "1 of"_p >> ws >> selection ->* join<disjunction>
      | id
      | selection ->* join<conjunction>
      ;
    // clang-format on
    return expr(f, l, result);
  }

  symbol_table<expression> id;
};

/// Parses the "detection" attribute from a Sigma rule. See the Sigma wiki for
/// details: https://github.com/Neo23x0/sigma/wiki/Specification#detection
struct detection_parser : parser_base<detection_parser> {
  using attribute = expression;

  explicit detection_parser(const expression_map& exprs) : search_id{exprs} {
  }

  static expression to_expr(
    std::tuple<expression, std::vector<std::tuple<bool_operator, expression>>>
      expr) {
    auto& [x, xs] = expr;
    if (xs.empty())
      return x;
    // We split the expression chain at each OR node in order to take care of
    // operator precedance: AND binds stronger than OR.
    disjunction dis;
    auto con = conjunction{x};
    for (auto& [op, expr] : xs)
      if (op == bool_operator::logical_and) {
        con.emplace_back(std::move(expr));
      } else if (op == bool_operator::logical_or) {
        TENZIR_ASSERT(!con.empty());
        if (con.size() == 1)
          dis.emplace_back(std::move(con[0]));
        else
          dis.emplace_back(std::move(con));
        con = conjunction{std::move(expr)};
      } else {
        TENZIR_ASSERT(!"negations must not exist here");
      }
    if (con.size() == 1)
      dis.emplace_back(std::move(con[0]));
    else
      dis.emplace_back(std::move(con));
    return dis.size() == 1 ? std::move(dis[0]) : expression{dis};
  };

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, expression& result) const {
    using namespace parser_literals;
    auto ws = ignore(*parsers::space);
    auto negate = [](expression x) {
      return negation{std::move(x)};
    };
    rule<Iterator, expression> expr;
    rule<Iterator, expression> group;
    // clang-format off
    group
      = '(' >> ws >> ref(expr) >> ws >> ')'
      | "not"_p >> ws >> '(' >> ws >> (ref(expr) ->* negate) >> ws >> ')'
      | "not"_p >> ws >> search_id ->* negate
      | search_id
      ;
    auto and_or
      = "or"_p  ->* [] { return bool_operator::logical_or; }
      | "and"_p  ->* [] { return bool_operator::logical_and; }
      ;
    expr
      = (group >> *(ws >> and_or >> ws >> ref(group)) >> ws) ->* to_expr
      ;
    // clang-format on
    auto p = expr >> parsers::eoi;
    return p(f, l, result);
  }

  search_id_symbol_table search_id;
};

/// Transforms a string that may contain Sigma glob wildcards into a pattern
/// with respective regular expression metacharacters. Sigma patterns are always
/// case-insensitive.
caf::expected<pattern>
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
  auto result = pattern::make(rx, {.case_insensitive = true});
  if (not result) {
    return result;
  }
  if (fmt.empty()) {
    return result;
  }
  return pattern::make(fmt::format(TENZIR_FMT_RUNTIME(fmt), result->string()),
                       result->options());
}

} // namespace

caf::expected<expression> parse_search_id(const data& yaml) {
  if (auto xs = try_as<record>(&yaml)) {
    conjunction result;
    for (auto& [key, rhs] : *xs) {
      auto keys = detail::split(key, "|");
      auto extractor = field_extractor{std::string{keys[0]}};
      auto op = relational_operator::equal;
      auto all = false;
      // Value transformation; identity (= no modifiers) by default.
      std::vector<std::function<caf::expected<data>(const data&)>> transforms;
      // Parse modifiers.
      for (auto i = keys.begin() + 1; i != keys.end(); ++i) {
        if (*i == "all") {
          all = true;
        } else if (*i == "lt") {
          op = relational_operator::less;
        } else if (*i == "lte") {
          op = relational_operator::less_equal;
        } else if (*i == "gt") {
          op = relational_operator::greater;
        } else if (*i == "gte") {
          op = relational_operator::greater_equal;
        } else if (*i == "contains") {
          auto to_re = [&](const data& d) -> caf::expected<data> {
            auto f = detail::overload{
              [&]<class T>(const T& x) -> caf::expected<data> {
                if constexpr (std::is_same_v<T, std::string>) {
                  auto result = transform_sigma_string(
                    detail::control_char_escape(x), ".*{}.*");
                  if (!result)
                    return std::move(result.error());
                  return std::move(*result);
                } else if constexpr (detail::is_any_v<T, subnet, list>) {
                  op = relational_operator::ni;
                  return x;
                } else {
                  return x;
                }
              }};
            return caf::visit(f, d);
          };
          transforms.emplace_back(to_re);
        } else if (*i == "base64") {
          auto encode = [](const data& x) -> caf::expected<data> {
            if (const auto* str = try_as<std::string>(&x)) {
              return detail::base64::encode(*str);
            }
            return caf::make_error(ec::type_clash, //
                                   "base64 only works with strings");
          };
          transforms.emplace_back(encode);
        } else if (*i == "base64offset") {
          auto encode = [](const data& x) -> caf::expected<data> {
            const auto* str = try_as<std::string>(&x);
            if (!str)
              return caf::make_error(ec::type_clash, //
                                     "base64offset only works with strings");
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
        } else if (*i == "utf16le" || *i == "wide") {
          return caf::make_error(ec::unimplemented, //
                                 "utf16le/wide not yet implemented");
          // FIXME: the attempt below doesn't work yet, but gives an idea on
          // what needs to be done algorithmically.
          auto convert = [](const data& x) -> caf::expected<data> {
            const auto* str = try_as<std::string>(&x);
            if (!str)
              return caf::make_error(ec::type_clash, //
                                     "utf16le/wide only works with strings");
            // Hand-roll conversion.
            std::string result;
            result.reserve(str->size() * 2);
            for (auto c : *str) {
              result.push_back(c);
              result.push_back(0x00);
            }
            return result;
          };
          transforms.emplace_back(convert);
        } else if (*i == "utf16be") {
          return caf::make_error(ec::unimplemented, //
                                 "utf16be not yet implemented");
        } else if (*i == "utf16") {
          return caf::make_error(ec::unimplemented, //
                                 "utf16 not yet implemented");
        } else if (*i == "startswith") {
          auto to_re = [](const data& d) -> caf::expected<data> {
            auto f = detail::overload{[](const auto& x) -> caf::expected<data> {
              auto str = detail::control_char_escape(to_string(x));
              auto result = transform_sigma_string(str, "^{}.*");
              if (!result)
                return std::move(result.error());
              return std::move(*result);
            }};
            return caf::visit(f, d);
          };
          transforms.emplace_back(to_re);
        } else if (*i == "endswith") {
          op = relational_operator::equal;
          auto to_re = [](const data& d) -> caf::expected<data> {
            auto f = detail::overload{[](const auto& x) -> caf::expected<data> {
              auto str = detail::control_char_escape(to_string(x));
              auto result = transform_sigma_string(str, ".*{}$");
              if (!result)
                return std::move(result.error());
              return std::move(*result);
            }};
            return caf::visit(f, d);
          };
          transforms.emplace_back(to_re);
        } else if (*i == "re") {
          op = relational_operator::equal;
          auto to_re = [](const data& d) -> caf::expected<data> {
            auto f = detail::overload{
              [](const auto& x) -> caf::expected<data> {
                auto str = to_string(x);
                auto result = transform_sigma_string(str, {});
                if (!result)
                  return std::move(result.error());
                if (str == result->string()) {
                  return str;
                }
                return std::move(*result);
              },
              [](const std::string& x) -> caf::expected<data> {
                auto result = pattern::make(x);
                if (!result)
                  return std::move(result.error());
                return std::move(*result);
              },
              [](pattern x) -> caf::expected<data> {
                return x;
              },
            };
            return caf::visit(f, d);
          };
          transforms.emplace_back(to_re);
        } else if (*i == "cidr") {
          // This modifier only requires adjusting the operator because tenzir
          // already parses strings as typed values.
          op = relational_operator::in;
        } else if (*i == "expand") {
          // TODO
          return caf::make_error(ec::unimplemented, "expand modifier not yet "
                                                    "implemented");
        }
      }
      // Helper to apply all modifiers over a value.
      auto modify = [&](const data& x) -> caf::expected<data> {
        auto result = x;
        for (const auto& f : transforms)
          if (auto x = f(result))
            result = std::move(*x);
          else
            return x.error();
        return result;
      };
      // Helper to create an expression from a (transformed) value.
      auto make_predicate_expr = [&](const data& value) -> expression {
        // Convert strings to case-insensitive patterns.
        if (auto str = try_as<std::string>(&value)) {
          if (auto pat = transform_sigma_string(*str, {}))
            return predicate{extractor, op, data{std::move(*pat)}};
        }
        // The modifier 'base64offset' is unique in that it creates
        // multiple values represented as list. If followed by 'contains', then
        // we have substring search on each value; otherwise we can use equality
        // comparison.
        if (auto xs = try_as<list>(&value)) {
          // Only 'base64offset' creates a list value. Lists are otherwise not
          // allowed as values.
          TENZIR_ASSERT(xs->size() == 3);
          disjunction result;
          for (const auto& x : *xs)
            result.emplace_back(predicate{extractor, op, x});
          return result;
        }
        // By default, we take the (potentially modified) operator.
        return predicate{extractor, op, value};
      };
      // Parse RHS.
      if (caf::holds_alternative<record>(rhs))
        return caf::make_error(ec::type_clash, "nested records not allowed");
      if (auto values = try_as<list>(&rhs)) {
        std::vector<expression> connective;
        for (const auto& value : *values) {
          if (caf::holds_alternative<list>(value))
            return caf::make_error(ec::type_clash, "nested lists disallowed");
          if (caf::holds_alternative<record>(value))
            return caf::make_error(ec::type_clash, "nested records disallowed");
          if (auto x = modify(value))
            connective.emplace_back(make_predicate_expr(*x));
          else
            return x.error();
        }
        auto expr = all ? expression{conjunction(std::move(connective))}
                        : expression{disjunction(std::move(connective))};
        result.emplace_back(hoist(std::move(expr)));
      } else {
        if (auto x = modify(rhs))
          result.emplace_back(make_predicate_expr(*x));
        else
          return x.error();
      }
    }
    return result.size() == 1 ? result[0] : result;
  } else if (auto xs = try_as<list>(&yaml)) {
    disjunction result;
    for (auto& search_id : *xs)
      if (auto expr = parse_search_id(search_id))
        result.push_back(std::move(*expr));
      else
        return expr.error();
    return result.size() == 1 ? result[0] : result;
    // } else if (auto x = try_as<std::string>(&yaml)) {
    //   return parse_search_id(*x);
  } else {
    return caf::make_error(
      ec::type_clash, fmt::format("search id '{}' not a list or record", yaml));
  }
}

caf::expected<expression> parse_rule(const data& yaml) {
  auto xs = try_as<record>(&yaml);
  if (!xs)
    return caf::make_error(ec::type_clash, "rule must be a record");
  // Extract detection attribute.
  const record* detection;
  if (auto i = xs->find("detection"); i == xs->end())
    return caf::make_error(ec::invalid_query, "no detection attribute");
  else
    detection = try_as<record>(&i->second);
  if (!detection)
    return caf::make_error(ec::type_clash, "detection not a record");
  // Resolve all named sub-expression except for "condition".
  expression_map exprs;
  for (auto& [key, value] : *detection) {
    if (key == "condition")
      continue;
    if (auto expr = parse_search_id(value))
      exprs[key] = std::move(*expr);
    else
      return expr.error();
  }
  // Extract condition.
  const std::string* condition;
  if (auto i = detection->find("condition"); i == detection->end())
    return caf::make_error(ec::invalid_query, "no condition key");
  else
    condition = try_as<std::string>(&i->second);
  if (!condition)
    return caf::make_error(ec::type_clash, "condition not a string");
  // Parse condition.
  expression result;
  detection_parser p{exprs};
  if (!p(*condition, result))
    return caf::make_error(ec::parse_error, "invalid condition syntax");
  return result;
}

} // namespace tenzir::plugins::sigma
