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
      = lit("~")   ->* [] { return match; }
      | lit("!~")  ->* [] { return not_match; }
      | lit("==")  ->* [] { return equal; }
      | lit("!=")  ->* [] { return not_equal; }
      | lit("<")   ->* [] { return less; }
      | lit("<=")  ->* [] { return less_equal; }
      | lit(">")   ->* [] { return greater; }
      | lit(">=")  ->* [] { return greater_equal; }
      | lit("in")  ->* [] { return in; }
      | lit("!in") ->* [] { return not_in; }
      | lit("ni")  ->* [] { return ni; }
      | lit("!ni") ->* [] { return not_ni; }
      | lit("[+")  ->* [] { return in; }
      | lit("[-")  ->* [] { return not_in; }
      | lit("+]")  ->* [] { return ni; }
      | lit("-]")  ->* [] { return not_ni; }
      ;
    auto rel_op
      = lit("||")  ->* [] { return logical_or; }
      | lit("&&")  ->* [] { return logical_and; }
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
