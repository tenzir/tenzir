#ifndef VAST_DETAIL_PARSER_EXPRESSION_H
#define VAST_DETAIL_PARSER_EXPRESSION_H

// Improves compile times significantly at the cost of predefining terminals.
#define BOOST_SPIRIT_NO_PREDEFINED_TERMINALS

#include <boost/spirit/include/qi.hpp>
#include <ze/detail/parser/value.h>
#include "vast/detail/ast/query.h"
#include "vast/util/parser/error_handler.h"
#include "vast/util/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

using util::parser::skipper;
namespace qi = boost::spirit::qi;
namespace ascii = boost::spirit::ascii;

template <typename Iterator>
struct expression : qi::grammar<Iterator, ast::query::expression(), skipper<Iterator>>
{
    expression(util::parser::error_handler<Iterator>& error_handler);

    qi::rule<Iterator, ast::query::expression(), skipper<Iterator>>
        expr;

    qi::rule<Iterator, ast::query::expr_operand(), skipper<Iterator>>
        unary, primary;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier;

    qi::symbols<char, ast::query::expr_operator>
        unary_op, binary_op;

    ze::detail::parser::value<Iterator> val;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
