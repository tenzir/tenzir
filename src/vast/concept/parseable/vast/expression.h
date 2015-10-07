#ifndef VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H
#define VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H

#include "vast/expression.h"
#include "vast/util/assert.h"

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/vast/data.h"
#include "vast/concept/parseable/vast/key.h"

namespace vast {

struct predicate_parser : parser<predicate_parser> {
  using attribute = predicate;

  static auto make() {
    // TODO: add full-fledged type parser
    auto type
      = "bool"_p     ->* []() -> vast::type { return type::boolean{}; }
      | "int"_p      ->* []() -> vast::type { return type::integer{}; }
      | "count"_p    ->* []() -> vast::type { return type::count{}; }
      | "real"_p     ->* []() -> vast::type { return type::real{}; }
      | "time"_p     ->* []() -> vast::type { return type::time_point{}; }
      | "duration"_p ->* []() -> vast::type { return type::time_duration{}; }
      | "string"_p   ->* []() -> vast::type { return type::string{}; }
      | "pattern"_p  ->* []() -> vast::type { return type::pattern{}; }
      | "addr"_p     ->* []() -> vast::type { return type::address{}; }
      | "subnet"_p   ->* []() -> vast::type { return type::subnet{}; }
      | "port"_p     ->* []() -> vast::type { return type::port{}; }
      ;
    auto to_type_extractor_operand = [](vast::type t) -> predicate::operand {
      return type_extractor{t};
    };
    auto to_schema_extractor_operand = [](key k) -> predicate::operand {
      return schema_extractor{k};
    };
    auto operand
      = parsers::data ->* [](data d) -> predicate::operand { return d; }
      | "&time"_p   ->* []() -> predicate::operand { return time_extractor{}; }
      | "&type"_p   ->* []() -> predicate::operand { return event_extractor{}; }
      | ':' >> type ->* to_type_extractor_operand
      | parsers::key ->* to_schema_extractor_operand
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

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, predicate& a) const {
    using namespace parsers;
    static auto p = make();
    return p.parse(f, l, a);
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

  template <typename Iterator>
  static auto make() {
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
          VAST_ASSERT(! con.empty());
          if (con.size() == 1)
            dis.emplace_back(std::move(con[0]));
          else
            dis.emplace_back(std::move(con));
          con = conjunction{std::move(std::get<1>(t))};
        } else {
          VAST_ASSERT(! "negations must not exist here");
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

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, unused_type) const {
    static auto p = make<Iterator>();
    return p.parse(f, l, unused);
  }

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, expression& a) const {
    static auto p = make<Iterator>();
    return p.parse(f, l, a);
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

#endif
