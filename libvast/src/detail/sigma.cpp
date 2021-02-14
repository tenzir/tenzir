/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/detail/sigma.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string.hpp"
#include "vast/detail/base64.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression_visitors.hpp"

#include <map>
#include <string>
#include <tuple>
#include <vector>

namespace vast::detail::sigma {

// TODO: A lot of code in here is directly copied from
// src/concept/parseable/expression.cpp. We should factor the implementation in
// the future.

namespace {

using expression_map = std::map<std::string, expression>;

/// Parses the 'detection' attribute from a Sigma rule. See the Sigma wiki for
/// details: https://github.com/Neo23x0/sigma/wiki/Specification#detection
struct detection_parser : parser<detection_parser> {
  using attribute = expression;

  explicit detection_parser(const expression_map& exprs) {
    for (auto& [key, value] : exprs)
      search_id.symbols.emplace(key, value);
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
        VAST_ASSERT(!con.empty());
        if (con.size() == 1)
          dis.emplace_back(std::move(con[0]));
        else
          dis.emplace_back(std::move(con));
        con = conjunction{std::move(expr)};
      } else {
        VAST_ASSERT(!"negations must not exist here");
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
    auto negate = [](expression x) { return negation{std::move(x)}; };
    rule<Iterator, expression> expr;
    rule<Iterator, expression> group;
    // clang-format off
    group
      = '(' >> ws >> ref(expr) >> ws >> ')'
      | "not"_p >> ws >> search_id ->* negate
      | "not"_p >> ws >> '(' >> ws >> (ref(expr) ->* negate) >> ws >> ')'
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

  symbol_table<expression> search_id;
};

caf::expected<expression> parse_search_id(const data& x) {
  if (auto xs = caf::get_if<record>(&x)) {
    conjunction result;
    for (auto& [key, rhs] : *xs) {
      auto keys = split(key, "|");
      auto extractor = field_extractor{std::string{keys[0]}};
      auto op = relational_operator::equal;
      // Parse modifiers. TODO: do this exhaustively.
      auto all = false;
      for (auto i = keys.begin() + 1; i != keys.end(); ++i) {
        if (*i == "all")
          all = true;
        if (*i == "contains")
          op = relational_operator::ni;
        if (*i == "endswith" || *i == "startswith")
          // Once we have regex support we should transform a lot of these
          // modifier in pattern qualifiers, e.g., `endswith` for a value X
          // should become /X$/.
          op = relational_operator::ni;
      }
      // Parse RHS.
      if (caf::holds_alternative<record>(rhs))
        return caf::make_error(ec::type_clash, "nested maps not allowed");
      if (auto values = caf::get_if<list>(&rhs)) {
        std::vector<expression> connective;
        for (auto& value : *values) {
          if (caf::holds_alternative<list>(value))
            return caf::make_error(ec::type_clash, "nested lists disallowed");
          if (caf::holds_alternative<record>(value))
            return caf::make_error(ec::type_clash, "nested records disallowed");
          connective.emplace_back(predicate{extractor, op, value});
        }
        auto expr = all ? expression{conjunction(std::move(connective))}
                        : expression{disjunction(std::move(connective))};
        result.emplace_back(std::move(expr));
      } else {
        result.emplace_back(predicate{std::move(extractor), op, rhs});
      }
    }
    return result;
  } else if (auto xs = caf::get_if<list>(&x)) {
    disjunction result;
    for (auto& search_id : *xs)
      if (auto expr = parse_search_id(search_id))
        result.push_back(std::move(*expr));
      else
        return expr.error();
    return result;
  } else {
    return caf::make_error(ec::type_clash, "search id not a list or record");
  }
}

} // namespace

caf::expected<expression> parse(const data& rule) {
  auto xs = caf::get_if<record>(&rule);
  if (!xs)
    return caf::make_error(ec::type_clash, "rule must be a record");
  // Extract detection attribute.
  const record* detection;
  if (auto i = xs->find("detection"); i == xs->end())
    return caf::make_error(ec::invalid_query, "no detection attribute");
  else
    detection = caf::get_if<record>(&i->second);
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
    condition = caf::get_if<std::string>(&i->second);
  if (!condition)
    return caf::make_error(ec::type_clash, "condition not a string");
  // Parse condition.
  expression result;
  detection_parser p{exprs};
  if (!p(*condition, result))
    return caf::make_error(ec::parse_error, "invalid condition syntax");
  return normalize(result);
}

} // namespace vast::detail::sigma
