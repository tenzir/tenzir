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
#include "vast/expression.hpp"

#include "vast/detail/assert.hpp"
#include "vast/detail/string.hpp"

namespace vast {

struct predicate_parser : parser<predicate_parser> {
  using attribute = predicate;

  static auto make() {
    using parsers::alnum;
    using parsers::chr;
    using namespace parser_literals;
    auto to_attr_extractor = [](std::string str) -> predicate::operand {
      return attribute_extractor{caf::atom_from_string(str)};
    };
    auto to_type_extractor = [](type t) -> predicate::operand {
      return type_extractor{t};
    };
    auto to_key_extractor = [](std::vector<std::string> xs) {
      // TODO: rather than doing all the work with the attributes, it would be
      // nice if the parser framework would just give us an iterator range over
      // the raw input. Then we wouldn't have to use expensive attributes and
      // could simply wrap a parser P into raw(P) or raw_str(P) to obtain a
      // range/string_view.
      auto key = detail::join(xs, ".");
      return predicate::operand{key_extractor{std::move(key)}};
    };
    auto id = +(alnum | chr{'_'} | chr{'-'});
    // A key cannot start with ':', othwise it would be interpreted as a type
    // extractor.
    auto key = !':'_p >> (+(alnum | chr{'_'} | chr{':'}) % '.');
    auto operand
      = parsers::data        ->* [](data d) -> predicate::operand { return d; }
      | '&' >> id            ->* to_attr_extractor
      | ':' >> parsers::type ->* to_type_extractor
      | key                  ->* to_key_extractor
      ;
    auto pred_op
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
    using raw_predicate =
      std::tuple<predicate::operand, relational_operator, predicate::operand>;
    auto to_predicate = [](raw_predicate t) -> predicate {
      return {std::move(std::get<0>(t)), std::get<1>(t),
              std::move(std::get<2>(t))};
    };
    auto ws = ignore(*parsers::space);
    auto pred
      = (operand >> ws >> pred_op >> ws >> operand) ->* to_predicate;
      ;
    return pred;
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
    using raw_expr =
      std::tuple<
        expression,
        std::vector<std::tuple<boolean_operator, expression>>
      >;
    // Converts a "raw" chain of sub-expressions and transforms it into an
    // expression tree.
    auto to_expr = [](raw_expr t) -> expression {
      auto& first = std::get<0>(t);
      auto& rest = std::get<1>(t);
      if (rest.empty())
        return first;
      // We split the expression chain at each OR node in order to take care of
      // operator precedance: AND binds stronger than OR.
      disjunction dis;
      auto con = conjunction{first};
      for (auto& t : rest)
        if (std::get<0>(t) == logical_and) {
          con.emplace_back(std::move(std::get<1>(t)));
        } else if (std::get<0>(t) == logical_or) {
          VAST_ASSERT(!con.empty());
          if (con.size() == 1)
            dis.emplace_back(std::move(con[0]));
          else
            dis.emplace_back(std::move(con));
          con = conjunction{std::move(std::get<1>(t))};
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
      = (group >> *(ws >> and_or >> ws >> group)) ->* to_expr
      ;
    return expr;
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, unused_type) const {
    static auto p = make<Iterator>();
    return p(f, l, unused);
  }

  template <class Iterator>
  bool parse(Iterator& f, const Iterator& l, expression& a) const {
    static auto p = make<Iterator>();
    return p(f, l, a);
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

