#ifndef VAST_DETAIL_PARSER_CLAUSE_DEFINITION_H
#define VAST_DETAIL_PARSER_CLAUSE_DEFINITION_H

#include "vast/detail/parser/query.h"

namespace vast {
namespace detail {
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
    qi::repeat_type repeat;
    qi::alpha_type alpha;
    qi::alnum_type alnum;
    qi::ulong_type ulong;

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
        ("count", ze::uint_type)
        ("double", ze::double_type)
        ("duration", ze::time_range_type)
        ("time", ze::time_point_type)
        ("string", ze::string_type)
        ("vector", ze::vector_type)
        ("set", ze::set_type)
        ("table", ze::table_type)
        ("record", ze::record_type)
        ("addr", ze::address_type)
        ("prefix", ze::prefix_type)
        ("port", ze::port_type)
        ;

    qry
        =   clause
        >>  *(boolean_op > clause)
        ;

    clause
        =   tag_clause
        |   type_clause
        |   offset_clause
        |   event_clause
        |   ('!' > not_clause)
        ;

    tag_clause
        =   '&' > identifier
        >   clause_op
        >   expr
        ;

    type_clause
        =   ':' > type
        >   clause_op
        >   expr
        ;

    offset_clause
        =   '@'
        >   ulong % ','
        >   clause_op
        >   expr
        ;

    event_clause
        =   glob >> *('$' > identifier)
        >   clause_op
        >   expr
        ;

    not_clause
        =   clause
        ;

    identifier
        =   raw[lexeme[(alpha | '_') >> *(alnum | '_' )]]
        ;

    // Supports currently only one level of scoping.
    glob
        =   raw[lexeme[
                (alpha | '_' | '*' | '?')
            >> *(alnum | '_' | '*' | '?')
            >> -(   repeat(2)[':']
                >   (alpha | '_' | '*' | '?')
                >> *(alnum | '_' | '*' | '?')
                )
            ]]
        ;

    event_name
        =   raw[lexeme[ ((alpha | '_') >> *(alnum | '_' )) % repeat(2)[':'] ]]
        ;

    BOOST_SPIRIT_DEBUG_NODES(
        (qry)
        (clause)
        (tag_clause)
        (type_clause)
        (offset_clause)
        (event_clause)
        (identifier)
    );

    error_handler.set(qry, _4, _3);

    boolean_op.name("binary boolean operator");
    clause_op.name("binary clause operator");
    type.name("type");
    qry.name("query");
    clause.name("clause");
    tag_clause.name("tag clause");
    offset_clause.name("offset clause");
    type_clause.name("type clause");
    event_clause.name("event clause");
    not_clause.name("negated clause");
    identifier.name("identifier");
}

} // namespace ast
} // namespace detail
} // namespace vast

#endif
