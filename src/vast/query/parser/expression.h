#ifndef VAST_QUERY_PARSER_EXPRESSION_H
#define VAST_QUERY_PARSER_EXPRESSION_H

#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include "vast/query/ast.h"
#include "vast/query/parser/error_handler.h"
#include "vast/query/parser/skipper.h"
#include <vector>

namespace vast {
namespace query {
namespace parser {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

template <typename Iterator>
struct expression : qi::grammar<Iterator, ast::expression(), skipper<Iterator>>
{
    expression(error_handler<Iterator>& error_handler);

    qi::rule<Iterator, ast::expression(), skipper<Iterator>>
        expr;

    qi::rule<Iterator, ast::expr_operand(), skipper<Iterator>>
        unary_expr, primary_expr;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier;

    qi::symbols<char, ast::expr_operator>
        unary_op, binary_op;

    qi::symbols<char>
        keywords;
};

} // namespace ast
} // namespace query
} // namespace vast

#endif
