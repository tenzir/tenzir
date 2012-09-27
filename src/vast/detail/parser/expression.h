#ifndef VAST_DETAIL_PARSER_EXPRESSION_H
#define VAST_DETAIL_PARSER_EXPRESSION_H

// Improves compile times significantly at the cost of predefining terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include <ze/detail/parser/value.h>
#include "vast/detail/ast/query.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

template <typename Iterator>
struct expression : qi::grammar<Iterator, ast::query::expression(), skipper<Iterator>>
{
    expression(error_handler<Iterator>& on_error);

    qi::rule<Iterator, ast::query::expression(), skipper<Iterator>>
        expr;

    qi::rule<Iterator, ast::query::expr_operand(), skipper<Iterator>>
        unary, primary;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier;

    qi::symbols<char, arithmetic_operator>
        unary_op, binary_op;

    ze::detail::parser::value<Iterator> val;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
