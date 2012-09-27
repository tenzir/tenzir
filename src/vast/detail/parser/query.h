#ifndef VAST_DETAIL_PARSER_CLAUSE_H
#define VAST_DETAIL_PARSER_CLAUSE_H

#include "vast/detail/parser/expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct query : qi::grammar<Iterator, ast::query::query(), skipper<Iterator>>
{
    query(error_handler<Iterator>& on_error);

    qi::rule<Iterator, ast::query::query(), skipper<Iterator>>
        qry;

    qi::rule<Iterator, ast::query::clause(), skipper<Iterator>>
        clause;

    qi::rule<Iterator, ast::query::tag_clause(), skipper<Iterator>>
        tag_clause;

    qi::rule<Iterator, ast::query::type_clause(), skipper<Iterator>>
        type_clause;

    qi::rule<Iterator, ast::query::offset_clause(), skipper<Iterator>>
        offset_clause;

    qi::rule<Iterator, ast::query::event_clause(), skipper<Iterator>>
        event_clause;

    qi::rule<Iterator, ast::query::negated_clause(), skipper<Iterator>>
        not_clause;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier, glob, event_name;

    qi::symbols<char, relational_operator>
        clause_op;

    qi::symbols<char, boolean_operator>
        boolean_op;

    qi::symbols<char, ze::value_type>
        type;

    expression<Iterator> expr;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
