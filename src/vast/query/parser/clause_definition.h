#ifndef VAST_QUERY_PARSER_CLAUSE_DEFINITION_H
#define VAST_QUERY_PARSER_CLAUSE_DEFINITION_H

#include "vast/query/parser/clause.h"

namespace vast {
namespace query {
namespace parser {

template <typename Iterator>
clause<Iterator>::clause(util::parser::error_handler<Iterator>& error_handler)
  : clause::base_type(query)
  , expr(error_handler)
{
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;

    qi::raw_type raw;
    qi::lexeme_type lexeme;
    qi::alpha_type alpha;
    qi::alnum_type alnum;

    using qi::on_error;
    using qi::fail;

    binary_query_op.add
        ("||", ast::logical_or)
        ("&&", ast::logical_and)
        ;

    unary_query_op.add
        ("!", ast::logical_not)
        ;

    binary_clause_op.add
        ("~",  ast::match)
        ("==", ast::equal)
        ("!=", ast::not_equal)
        ("<",  ast::less)
        ("<=", ast::less_equal)
        (">",  ast::greater)
        (">=", ast::greater_equal)
        ;

    type.add
        ("bool", ast::bool_type)
        ("int", ast::int_type)
        ("uint", ast::uint_type)
        ("double", ast::double_type)
        ("duration", ast::duration_type)
        ("timepoint", ast::timepoint_type)
        ("string", ast::string_type)
        ("vector", ast::vector_type)
        ("set", ast::set_type)
        ("table", ast::table_type)
        ("record", ast::record_type)
        ("address", ast::address_type)
        ("prefix", ast::prefix_type)
        ("port", ast::port_type)
        ;

    query
        =   unary_clause
        >>  *(binary_query_op > unary_clause)
        ;

    unary_clause
        =   event_clause
        |   type_clause
        |   (unary_query_op > unary_clause)
        ;

    event_clause
        =   identifier > '.' > identifier
        >   binary_clause_op
        >   expr;

    type_clause
        =   lexeme['@' > type]
        >   binary_clause_op > expr;
        ;

    identifier
        =   raw[lexeme[(alpha | '_') >> *(alnum | '_')]]
        ;

    BOOST_SPIRIT_DEBUG_NODES(
        (query)
        (unary_clause)
        (type_clause)
        (event_clause)
        (identifier)
    );

    on_error<fail>(query, error_handler.functor()(_4, _3));
    on_error<fail>(unary_clause, error_handler.functor()(_4, _3));
    on_error<fail>(event_clause, error_handler.functor()(_4, _3));
    on_error<fail>(type_clause, error_handler.functor()(_4, _3));

    binary_query_op.name("binary query operator");
    unary_query_op.name("unary query operator");
    binary_clause_op.name("binary clause operator");
    query.name("query");
    unary_clause.name("unary clause");
    event_clause.name("event clause");
    type_clause.name("type clause");
    identifier.name("identifier");
}

} // namespace ast
} // namespace query
} // namespace vast

#endif
