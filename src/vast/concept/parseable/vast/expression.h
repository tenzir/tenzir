#ifndef VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H
#define VAST_CONCEPT_PARSEABLE_VAST_EXPRESSION_H

#include "vast/expression.h"

#include "vast/concept/parseable/core.h"
#include "vast/concept/parseable/string/char_class.h"
#include "vast/concept/parseable/vast/data.h"
#include "vast/concept/parseable/vast/detail/expression.h"

namespace vast {
namespace ast {
namespace expr {} // namespace expr
} // namespace ast

struct expression_parser : vast::parser<expression_parser> {
  using attribute = expression;

  template <typename Iterator>
  static auto make() {
    using namespace parsers;
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
      = parsers::data | id
      ;
    auto pred
      = operand >> pred_op >> operand
      ;
    rule<Iterator> group; // TODO: use proper attribute.
    group
      = '(' >> group >> ')'
      | '!' >> group
      | pred
      ;
    auto expr
      = group >> *(rel_op >> group)
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
