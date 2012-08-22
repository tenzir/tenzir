#ifndef VAST_DETAIL_PARSER_EXPRESSION_DEFINITION_H
#define VAST_DETAIL_PARSER_EXPRESSION_DEFINITION_H

#include "vast/detail/parser/expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
expression<Iterator>::expression(error_handler<Iterator>& on_error)
  : expression::base_type(expr)
{
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;

    using qi::on_error;
    using qi::fail;

    binary_op.add
        ("+", ast::query::plus)
        ("-", ast::query::minus)
        ("*", ast::query::times)
        ("/", ast::query::divide)
        ("%", ast::query::mod)
        ("|", ast::query::bitwise_or)
        ("^", ast::query::bitwise_xor)
        ("&", ast::query::bitwise_and)
        ;

    unary_op.add
        ("+", ast::query::positive)
        ("-", ast::query::negative)
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
        =   val
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

} // namespace ast
} // namespace detail
} // namespace vast

#endif
