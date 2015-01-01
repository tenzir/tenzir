#ifndef VAST_DETAIL_PARSER_DATA_EXPRESSION_H
#define VAST_DETAIL_PARSER_DATA_EXPRESSION_H

#include "vast/detail/parser/boost.h"

#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"
#include "vast/detail/parser/data.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

template <typename Iterator>
struct data_expression
  : qi::grammar<Iterator, ast::query::data_expr(), skipper<Iterator>>
{
  data_expression(error_handler<Iterator>& on_error)
    : data_expression::base_type(expr)
  {
      qi::_3_type _3;
      qi::_4_type _4;

      binary_op.add
          ("+", plus)
          ("-", minus)
          ("*", times)
          ("/", divides)
          ("%", mod)
          ("|", bitwise_or)
          ("^", bitwise_xor)
          ("&", bitwise_and)
          ;

      unary_op.add
          ("+", positive)
          ("-", negative)
          ;

      expr
          =   unary
          >>  *(binary_op >> unary)
          ;

      unary
          =   primary
          |   (unary_op > unary)
          ;

      primary
          =   dta
          |   ('(' > expr > ')')
          ;

      BOOST_SPIRIT_DEBUG_NODES(
          (expr)
          (unary)
          (primary)
      );

      on_error.set(expr, _4, _3);

      binary_op.name("binary expression operator");
      unary_op.name("unary expression operator");
      expr.name("expression");
      unary.name("unary expression");
      primary.name("primary expression");
  }

  qi::rule<Iterator, ast::query::data_expr(), skipper<Iterator>>
      expr;

  qi::rule<Iterator, ast::query::expr_operand(), skipper<Iterator>>
      unary, primary;

  qi::rule<Iterator, std::string(), skipper<Iterator>>
      identifier;

  qi::symbols<char, arithmetic_operator>
      unary_op, binary_op;

  data<Iterator> dta;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
