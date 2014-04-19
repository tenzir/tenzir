#ifndef VAST_DETAIL_PARSER_QUERY_H
#define VAST_DETAIL_PARSER_QUERY_H

#include "vast/detail/parser/expression.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
struct query : qi::grammar<Iterator, ast::query::query(), skipper<Iterator>>
{
    query(error_handler<Iterator>& on_error);

    qi::rule<Iterator, ast::query::query(), skipper<Iterator>>
        start;

    qi::rule<Iterator, ast::query::group(), skipper<Iterator>>
        group;

    qi::rule<Iterator, ast::query::predicate(), skipper<Iterator>>
        pred;

    qi::rule<Iterator, ast::query::tag_predicate(), skipper<Iterator>>
        tag_pred;

    qi::rule<Iterator, ast::query::type_predicate(), skipper<Iterator>>
        type_pred;

    qi::rule<Iterator, ast::query::offset_predicate(), skipper<Iterator>>
        offset_pred;

    qi::rule<Iterator, ast::query::schema_predicate(), skipper<Iterator>>
        schema_pred;

    qi::rule<Iterator, ast::query::negated_predicate(), skipper<Iterator>>
        not_pred;

    qi::rule<Iterator, std::string(), skipper<Iterator>>
        identifier, glob, event_name;

    qi::symbols<char, relational_operator>
        pred_op;

    qi::symbols<char, boolean_operator>
        boolean_op;

    qi::symbols<char, value_type>
        type;

    value_expression<Iterator> value_expr;
};

} // namespace ast
} // namespace detail
} // namespace vast

#endif
