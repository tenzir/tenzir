#ifndef VAST_QUERY_PARSER_EXPRESSION_DEFINITION_H
#define VAST_QUERY_PARSER_EXPRESSION_DEFINITION_H

#include "vast/query/parser/expression.h"
#include <boost/spirit/include/phoenix_function.hpp>

namespace vast {
namespace query {
namespace parser {

template <typename Iterator>
expression<Iterator>::expression(error_handler<Iterator>& error_handler)
  : expression::base_type(expr)
{
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;

    qi::uint_type uint_;
    qi::raw_type raw;
    qi::lexeme_type lexeme;
    qi::alpha_type alpha;
    qi::alnum_type alnum;
    qi::bool_type bool_;

    using qi::on_error;
    using qi::fail;
    using boost::phoenix::function;

    typedef function<parser::error_handler<Iterator>> error_handler_function;

    binary_op.add
        ("+",  ast::plus)
        ("-",  ast::minus)
        ("*",  ast::times)
        ("/",  ast::divide)
        ("%",  ast::mod)
        ;

    unary_op.add
        ("|", ast::bitwise_or)
        ("^", ast::bitwise_xor)
        ("&", ast::bitwise_and)
        ("+", ast::positive)
        ("-", ast::negative)
        ;

    keywords.add
        ("T")
        ("F")
        ;

    expr 
        =   unary_expr
        >>  *(binary_op > unary_expr)
        ;

    unary_expr 
        =   primary_expr
        |   (unary_op > unary_expr)
        ;

    primary_expr 
        =   identifier
        |   bool_
        |   ('(' > expr > ')')
        ;

    identifier 
        =   !keywords
        >>  raw[lexeme[(alpha | '_') >> *(alnum | '_')]]
        ;

    BOOST_SPIRIT_DEBUG_NODES(
        (expr)
        (unary_expr)
        (primary_expr)
        (identifier)
    );

    on_error<fail>(expr,
                   error_handler_function(error_handler)(
                       "error! expecting ", _4, _3));

}

} // namespace ast
} // namespace query
} // namespace vast

#endif
