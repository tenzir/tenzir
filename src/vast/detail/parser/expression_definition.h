#ifndef VAST_DETAIL_PARSER_EXPRESSION_DEFINITION_H
#define VAST_DETAIL_PARSER_EXPRESSION_DEFINITION_H

#include "vast/detail/parser/expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
expression<Iterator>::expression(
    util::parser::error_handler<Iterator>& error_handler)
  : expression::base_type(expr)
{
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;

    using qi::on_error;
    using qi::fail;

    binary_op.add
        ("+", ast::plus)
        ("-", ast::minus)
        ("*", ast::times)
        ("/", ast::divide)
        ("%", ast::mod)
        ("|", ast::bitwise_or)
        ("^", ast::bitwise_xor)
        ("&", ast::bitwise_and)
        ;

    unary_op.add
        ("+", ast::positive)
        ("-", ast::negative)
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

    error_handler.set(expr, _4, _3);

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
