#ifndef VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H
#define VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H

#include "vast/expression.h"

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/vast/data.h"
#include "vast/concept/parseable/vast/detail/expression.h"

namespace vast {

struct expression_parser : vast::parser<expression_parser> {
  using attribute = expression;

  template <typename Iterator>
  static auto make() {
    using namespace parsers;
    using predicate_tuple =
      std::tuple<predicate::operand, relational_operator, predicate::operand>;
    using group_tuple =
      std::tuple<
        expression,
        std::vector<std::tuple<relational_operator, expression>>
      >;
    auto pred_to_expr = [](predicate_tuple t) -> predicate {
      return {std::move(std::get<0>(t)), std::get<1>(t),
              std::move(std::get<2>(t))};
    };
    auto str_to_operand = [](std::string str) -> predicate::operand {
      return {}; // TODO
    };
    auto group_to_expr = [](group_tuple t) -> expression {
      return {}; // TODO
    };
    auto id
      = (alpha | '_' | '&' | ':') >> *(alnum | '_' | '.' | ':')
      ;
    auto pred_op
      = "~"_p   ->* [] { return match; }
      | "!~"_p  ->* [] { return not_match; }
      | "=="_p  ->* [] { return equal; }
      | "!="_p  ->* [] { return not_equal; }
      | "<"_p   ->* [] { return less; }
      | "<="_p  ->* [] { return less_equal; }
      | ">"_p   ->* [] { return greater; }
      | ">="_p  ->* [] { return greater_equal; }
      | "in"_p  ->* [] { return in; }
      | "!in"_p ->* [] { return not_in; }
      | "ni"_p  ->* [] { return ni; }
      | "!ni"_p ->* [] { return not_ni; }
      | "[+"_p  ->* [] { return in; }
      | "[-"_p  ->* [] { return not_in; }
      | "+]"_p  ->* [] { return ni; }
      | "-]"_p  ->* [] { return not_ni; }
      ;
    auto rel_op
      = "||"_p  ->* [] { return logical_or; }
      | "&&"_p  ->* [] { return logical_and; }
      ;
    auto operand
      = parsers::data ->* [](data d) -> predicate::operand { return d; }
      | id            ->* str_to_operand
      ;
    auto pred
      = (operand >> pred_op >> operand) ->* pred_to_expr;
      ;
    rule<Iterator, expression> group;
    group
      = '(' >> group >> ')'
      | '!' >> group
      | pred
      ;
    auto expr
      = (group >> *(rel_op >> group)) ->* group_to_expr
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
    using namespace parsers;
    static auto p = make<Iterator>();
    return p.parse(f, l, a);
  }
};

template <>
struct parser_registry<expression> {
  using type = detail::expression_parser;
};

namespace parsers {

static auto const expr = make_parser<vast::expression>();

} // namespace parsers
} // namespace vast

#endif
