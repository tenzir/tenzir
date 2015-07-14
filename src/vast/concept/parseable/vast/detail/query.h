#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_QUERY_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_QUERY_H

#include "vast/concept/parseable/vast/detail/data_expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct query : qi::grammar<Iterator, ast::query::query_expr(), skipper<Iterator>>
{
  query(error_handler<Iterator>& on_error)
    : query::base_type{expr},
      data_expr{on_error}
  {
    qi::_3_type _3;
    qi::_4_type _4;
    qi::raw_type raw;
    qi::lexeme_type lexeme;
    qi::alpha_type alpha;
    qi::alnum_type alnum;

    boolean_op.add
      ("||", logical_or)
      ("&&", logical_and)
      ;

    pred_op.add
      ("~",   match)
      ("!~",  not_match)
      ("==",  equal)
      ("!=",  not_equal)
      ("<",   less)
      ("<=",  less_equal)
      (">",   greater)
      (">=",  greater_equal)
      ("in",  in)
      ("!in", not_in)
      ("ni",  ni)
      ("!ni", not_ni)
      ("[+",  in)
      ("[-",  not_in)
      ("+]",  ni)
      ("-]",  not_ni)
      ;

    expr
      =   group >> *(boolean_op > group)
      ;

    group
      =   neg_expr
      |   '(' >> expr >> ')'
      |   pred
      ;

    neg_expr
      =   '!' >> expr
      ;

    pred
      =   (data_expr | identifier)
      >   pred_op
      >   (data_expr | identifier)
      ;

    identifier
      =   raw[lexeme[(alpha | '_' | '&' | ':') >> *(alnum | '_' | '.' | ':')]]
      ;

    BOOST_SPIRIT_DEBUG_NODES(
        (expr)
        (group)
        (neg_expr)
        (pred)
        (identifier)
        );

    on_error.set(expr, _4, _3);

    boolean_op.name("binary boolean operator");
    pred_op.name("predicate operator");
    expr.name("expression");
    neg_expr.name("negated expression");
    pred.name("predicate");
    identifier.name("identifier");
  }

  qi::rule<Iterator, ast::query::query_expr(), skipper<Iterator>> expr;
  qi::rule<Iterator, ast::query::group(), skipper<Iterator>> group;
  qi::rule<Iterator, ast::query::negated(), skipper<Iterator>> neg_expr;
  qi::rule<Iterator, ast::query::predicate(), skipper<Iterator>> pred;
  qi::rule<Iterator, std::string(), skipper<Iterator>> identifier;

  qi::symbols<char, relational_operator> pred_op;
  qi::symbols<char, boolean_operator> boolean_op;

  data_expression<Iterator> data_expr;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
