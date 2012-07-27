#ifndef VAST_DETAIL_PARSER_CLAUSE_H
#define VAST_DETAIL_PARSER_CLAUSE_H

#include "vast/detail/parser/expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct query : qi::grammar<Iterator, ast::query(), skipper<Iterator>>
{
    query(util::parser::error_handler<Iterator>& error_handler);

    qi::rule<Iterator, ast::query(), skipper<Iterator>>
        qry;

    qi::rule<Iterator, ast::clause(), skipper<Iterator>>
        clause;

    qi::rule<Iterator, ast::type_clause(), skipper<Iterator>>
        type_clause;

    qi::rule<Iterator, ast::event_clause(), skipper<Iterator>>
        event_clause;

    qi::rule<Iterator, ast::negated_clause(), skipper<Iterator>>
        not_clause;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier, glob, event_name;

    qi::symbols<char, ast::clause_operator>
        clause_op;

    qi::symbols<char, ast::boolean_operator>
        boolean_op;

    qi::symbols<char, ze::value_type>
        type;

    expression<Iterator> expr;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
