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

#pragma once

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/type.hpp"
#include "vast/data.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"
#include "vast/expression.hpp"
#include "vast/type.hpp"

namespace vast {

struct predicate_parser : parser<predicate_parser> {
  using attribute = predicate;

  using predicate_tuple = std::tuple<predicate::operand, relational_operator,
                                     predicate::operand>;

  static predicate to_predicate(predicate_tuple xs);

  static predicate::operand to_key_extractor(std::vector<std::string> xs);

  static predicate::operand to_attr_extractor(std::string x);

  static predicate::operand to_type_extractor(type x);

  static predicate::operand to_data_operand(data x);

  static predicate to_data_predicate(data x);

  static auto make() {
    // clang-format off
    using parsers::alnum;
    using parsers::chr;
    using namespace parser_literals;
    auto id = +(alnum | chr{'_'} | chr{'-'});
    // A key cannot start with ':', othwise it would be interpreted as a type
    // extractor.
    auto key = !':'_p >> (+(alnum | chr{'_'} | chr{':'}) % '.');
    auto operand
      = parsers::data ->* to_data_operand
      | '#' >> id ->* to_attr_extractor
      | ':' >> parsers::type ->* to_type_extractor
      | key ->* to_key_extractor
      ;
    auto operation
      = "~"_p   ->* [] { return match; }
      | "!~"_p  ->* [] { return not_match; }
      | "=="_p  ->* [] { return equal; }
      | "!="_p  ->* [] { return not_equal; }
      | "<="_p  ->* [] { return less_equal; }
      | "<"_p   ->* [] { return less; }
      | ">="_p  ->* [] { return greater_equal; }
      | ">"_p   ->* [] { return greater; }
      | "in"_p  ->* [] { return in; }
      | "!in"_p ->* [] { return not_in; }
      | "ni"_p  ->* [] { return ni; }
      | "!ni"_p ->* [] { return not_ni; }
      | "[+"_p  ->* [] { return in; }
      | "[-"_p  ->* [] { return not_in; }
      | "+]"_p  ->* [] { return ni; }
      | "-]"_p  ->* [] { return not_ni; }
      ;
    auto ws = ignore(*parsers::space);
    auto pred
      = (operand >> ws >> operation >> ws >> operand) ->* to_predicate
      | parsers::data ->* to_data_predicate;
      ;
    return pred;
    // clang-format on
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, predicate& a) const {
    using namespace parsers;
    static auto p = make();
    return p(f, l, a);
  }
};

template <>
struct parser_registry<predicate> {
  using type = predicate_parser;
};

namespace parsers {

static auto const predicate = make_parser<vast::predicate>();

} // namespace parsers

struct expression_parser : parser<expression_parser> {
  using attribute = expression;

  template <class Iterator>
  static auto make() {
    using namespace parser_literals;
    using chain = std::vector<std::tuple<bool_operator, expression>>;
    using raw_expr = std::tuple<expression, chain>;
    // Converts a "raw" chain of sub-expressions and transforms it into an
    // expression tree.
    auto to_expr = [](raw_expr expr) -> expression {
      auto& [x, xs] = expr;
      if (xs.empty())
        return x;
      // We split the expression chain at each OR node in order to take care of
      // operator precedance: AND binds stronger than OR.
      disjunction dis;
      auto con = conjunction{x};
      for (auto& [op, expr] : xs)
        if (op == logical_and) {
          con.emplace_back(std::move(expr));
        } else if (op == logical_or) {
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
    auto ws = ignore(*parsers::space);
    auto negate_pred = [](predicate p) { return negation{expression{p}}; };
    auto negate_expr = [](expression expr) { return negation{expr}; };
    rule<Iterator, expression> expr;
    rule<Iterator, expression> group;
    // clang-format off
    group
      = '(' >> ws >> expr >> ws >> ')'
      | '!' >> ws >> parsers::predicate ->* negate_pred
      | '!' >> ws >> '(' >> ws >> (expr ->* negate_expr) >> ws >> ')'
      | parsers::predicate
      ;
    auto and_or
      = "||"_p  ->* [] { return logical_or; }
      | "&&"_p  ->* [] { return logical_and; }
      ;
    expr
      = (group >> *(ws >> and_or >> ws >> group) >> ws) ->* to_expr
      ;
    // clang-format on
    return expr >> parsers::eoi;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    static auto p = make<Iterator>();
    return p(f, l, x);
  }
};

template <>
struct parser_registry<expression> {
  using type = expression_parser;
};

namespace parsers {

static auto const expr = make_parser<vast::expression>();

} // namespace parsers
} // namespace vast
