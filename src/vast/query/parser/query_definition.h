#ifndef VAST_QUERY_PARSER_CLAUSE_DEFINITION_H
#define VAST_QUERY_PARSER_CLAUSE_DEFINITION_H

#include "vast/query/parser/query.h"

namespace vast {
namespace query {
namespace parser {

template <typename Iterator>
query<Iterator>::query(util::parser::error_handler<Iterator>& error_handler)
  : query::base_type(qry)
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

    boolean_op.add
        ("||", ast::logical_or)
        ("&&", ast::logical_and)
        ;

    clause_op.add
        ("~",   ast::match)
        ("!~",  ast::not_match)
        ("==",  ast::equal)
        ("!=",  ast::not_equal)
        ("<",   ast::less)
        ("<=",  ast::less_equal)
        (">",   ast::greater)
        (">=",  ast::greater_equal)
        ("in",  ast::in)
        ("!in", ast::not_in)
        ;

    type.add
        ("bool", ze::bool_type)
        ("int", ze::int_type)
        ("uint", ze::uint_type)
        ("double", ze::double_type)
        ("duration", ze::duration_type)
        ("timepoint", ze::timepoint_type)
        ("string", ze::string_type)
        ("vector", ze::vector_type)
        ("set", ze::set_type)
        ("table", ze::table_type)
        ("record", ze::record_type)
        ("address", ze::address_type)
        ("prefix", ze::prefix_type)
        ("port", ze::port_type)
        ;

    qry
        =   clause
        >>  *(boolean_op > clause)
        ;

    clause
        =   type_clause
        |   event_clause
        |   ('!' > not_clause)
        ;

    type_clause
        =   lexeme['@' > type]
        >   clause_op
        >   expr;
        ;

    event_clause
        =   identifier > '.' > identifier
        >   clause_op
        >   expr;

    not_clause
        =   clause
        ;

    identifier
        =   raw[lexeme[(alpha | '_') >> *(alnum | '_')]]
        ;

    BOOST_SPIRIT_DEBUG_NODES(
        (qry)
        (clause)
        (type_clause)
        (event_clause)
        (identifier)
    );

    error_handler.set(qry, _4, _3);

    boolean_op.name("binary boolean operator");
    clause_op.name("binary clause operator");
    type.name("type");
    qry.name("query");
    clause.name("clause");
    event_clause.name("event clause");
    type_clause.name("type clause");
    not_clause.name("negated clause");
    identifier.name("identifier");
}

} // namespace ast
} // namespace query
} // namespace vast

#endif
