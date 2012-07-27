#ifndef VAST_META_PARSER_TAXONOMY_DEFINITION_H
#define VAST_META_PARSER_TAXONOMY_DEFINITION_H

#include "vast/meta/exception.h"
#include "vast/meta/parser/taxonomy.h"
#include "vast/logger.h"
#include <boost/spirit/include/phoenix.hpp>

namespace vast {
namespace meta {
namespace parser {

template <typename Iterator>
taxonomy<Iterator>::taxonomy(
    util::parser::error_handler<Iterator>& error_handler)
  : taxonomy::base_type(tax)
{
    using boost::phoenix::begin;
    using boost::phoenix::end;
    using boost::phoenix::construct;
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;
    qi::_val_type _val;
    qi::lexeme_type lexeme;
    qi::lit_type lit;
    qi::raw_type raw;
    qi::char_type char_;
    namespace ascii = boost::spirit::ascii;
    ascii::space_type space;
    ascii::alnum_type alnum;
    ascii::alpha_type alpha;

    basic_type.add
        ("bool", ast::bool_type)
        ("int", ast::int_type)
        ("count", ast::uint_type)
        ("double", ast::double_type)
        ("interval", ast::duration_type)
        ("time", ast::timepoint_type)
        ("string", ast::string_type)
        ("pattern", ast::regex_type)
        ("addr", ast::address_type)
        ("subnet", ast::prefix_type)
        ("port", ast::port_type)
        ;

    auto type_decl_lambda = 
        [&](ast::type_declaration const& td, qi::unused_type, qi::unused_type)
        {
            if (types.find(td.name))
            {
                LOG(error, meta) << "duplicate type: " << td.name;
                throw semantic_exception("duplicate type");
            }
            else if (events.find(td.name))
            {
                LOG(error, meta) 
                    << "event with name '" << td.name << "' already exists";
                throw semantic_exception("invalid type name");
            }

            types.add(td.name, td.type);
        };

    auto event_decl_lambda =
        [&](ast::event_declaration const& ed, qi::unused_type,
            qi::unused_type)
        {
            if (events.find(ed.name))
            {
                LOG(error, meta) << "duplicate event: " << ed.name;
                throw semantic_exception("duplicate event");
            }
            else if (types.find(ed.name))
            {
                LOG(error, meta) 
                    << "type with name '" << ed.name << "' already exists";
                throw semantic_exception("invalid event name");
            }

            events.add(ed.name);
        };

    tax
        =   *stmt
        ;

    stmt
        =   type_decl   [_val = _1][type_decl_lambda]
        |   event_decl  [_val = _1][event_decl_lambda]
        ;

    event_decl
        =   lit("event")
        >   identifier
        >   '('
        >   -(argument % ',')
        >   ')'
        ;

    type_decl
        =   lit("type")
        >   identifier
        >   ':'
        >   type_info
        ;

    argument
        =   identifier
        >   ':'
        >   type_info
        >   -(+attr)
        ;

    attr
        =  lexeme
           [
                '&'
            >   identifier
            >   -(  '='
                 >  (( '"'
                       > *(char_ - '"')
                       > '"' ) // Extra parentheses removes a compiler warning.
                    |  +(char_ - space)
                    )
                 )
           ]
        ;

    type
        =   basic_type
        |   enum_
        |   vector
        |   set
        |   table
        |   record
        ;

    type_info
        =   raw[types]     [_val = construct<std::string>(begin(_1), end(_1))]
        |   type           [_val = _1]
        ;

    enum_
        =   lit("enum")
        >   '{'
        >   identifier % ','
        >   '}'
        ;

    vector
        =   lit("vector")
        >   "of"
        >   type_info
        ;

    set
        =   lit("set")
        >   '['
        >   type_info
        >   ']'
        ;

    table
        =   lit("table")
        >   '['
        >   type_info
        >   ']'
        >   "of"
        >   type_info
        ;

    record
        =   lit("record")
        >   '{'
        >   argument % ','
        >   '}'
        ;

    identifier
        =   alpha >> *(alnum | char_('_') | char_('-'))
        ;

    error_handler.set(tax, _4, _3);

    tax.name("taxonomy");
    stmt.name("statement");
    type_decl.name("type declaration");
    event_decl.name("event declaration");
    argument.name("argument");
    attr.name("attribute");
    type.name("type");
    type_info.name("type info");
    basic_type.name("basic type");
    enum_.name("enum type");
    vector.name("vector type");
    set.name("set type");
    table.name("table type");
    record.name("record type");
    identifier.name("identifier");
}

} // namespace parser
} // namespace meta
} // namespace vast

#endif
