#ifndef VAST_DETAIL_PARSER_SCHEMA_H
#define VAST_DETAIL_PARSER_SCHEMA_H

#include "vast/detail/ast/schema.h"
#include "vast/detail/parser/error_handler.h"
#include "vast/detail/parser/skipper.h"

namespace vast {
namespace detail {
namespace parser {

namespace qi = boost::spirit::qi;

template <typename Iterator>
struct schema : qi::grammar<Iterator, ast::schema::schema(), skipper<Iterator>>
{
  schema(error_handler<Iterator>& on_error)
    : schema::base_type(schema_)
  {
    using qi::unused_type;
    using boost::phoenix::at_c;
    using boost::phoenix::begin;
    using boost::phoenix::end;
    using boost::phoenix::construct;
    qi::_1_type _1;
    qi::_2_type _2;
    qi::_3_type _3;
    qi::_4_type _4;
    qi::_val_type _val;
    qi::lexeme_type lexeme;
    qi::raw_type raw;
    qi::repeat_type repeat;
    qi::lit_type lit;
    qi::char_type char_;
    namespace ascii = boost::spirit::ascii;
    ascii::space_type space;
    ascii::alnum_type alnum;
    ascii::alpha_type alpha;

    auto add_type =
      [=](std::string const& id, unused_type = {}, unused_type = {})
      {
        if (! user_type_.find(id))
          user_type_.add(id, id);
      };

    basic_type_.add
        ("bool",     ast::schema::bool_type)
        ("int",      ast::schema::int_type)
        ("count",    ast::schema::uint_type)
        ("real",     ast::schema::double_type)
        ("duration", ast::schema::time_frame_type)
        ("time",     ast::schema::time_point_type)
        ("string",   ast::schema::string_type)
        ("pattern",  ast::schema::regex_type)
        ("addr",     ast::schema::address_type)
        ("subnet",   ast::schema::prefix_type)
        ("port",     ast::schema::port_type)
        ;

    schema_
        =   *type_decl_
        ;

    type_decl_
        =   lit("type")
        >   identifier_   [at_c<0>(_val) = _1][add_type]
        >   '='
        >   type_         [at_c<1>(_val) = _1]
        ;

    argument_
        =   identifier_
        >   ':'
        >   type_
        ;

    attribute_
        =  lexeme
           [
                '&'
            >   identifier_
            >   -(  '='
                 >  (( '"'
                       > *(char_ - '"')
                       > '"' )
                    |  +(char_ - space)
                    )
                 )
           ]
        ;

    type_info_
        =   user_type_
        |   enum_
        |   vector_
        |   set_
        |   table_
        |   record_
        |   basic_type_
        ;

    type_
        =   type_info_
        >>  *attribute_
        ;

    enum_
        =   lit("enum")
        >   '{'
        >   identifier_ % ','
        >   '}'
        ;

    vector_
        =   lit("vector")
        >   '<'
        >   type_
        >   '>'
        ;

    set_
        =   lit("set")
        >   '<'
        >   type_
        >   '>'
        ;

    table_
        =   lit("table")
        >   '<'
        >   type_
        >   ','
        >   type_
        >   '>'
        ;

    record_
        =   lit("record")
        >   '{'
        >   argument_ % ','
        >   '}'
        ;

    identifier_
      = raw
        [
              alpha
          >> *(alnum | '_')
          >> *(repeat(2)[':'] > (alpha >> *(alnum | '_')))
        ]
      ;

    on_error.set(schema_, _4, _3);

    type_.name("type symbol");
    schema_.name("schema");
    type_decl_.name("type declaration");
    argument_.name("argument");
    attribute_.name("attribute");
    type_info_.name("type info");
    enum_.name("enum type");
    vector_.name("vector type");
    set_.name("set type");
    table_.name("table type");
    type_.name("type");
    record_.name("record type");
    identifier_.name("identifier");
  }

  qi::symbols<char, ast::schema::basic_type> basic_type_;
  qi::symbols<char, std::string> user_type_;
  qi::rule<Iterator, ast::schema::schema(), skipper<Iterator>> schema_;
  qi::rule<Iterator, ast::schema::type_declaration(), skipper<Iterator>> type_decl_;
  qi::rule<Iterator, ast::schema::argument_declaration(), skipper<Iterator>> argument_;
  qi::rule<Iterator, ast::schema::attribute(), skipper<Iterator>> attribute_;
  qi::rule<Iterator, ast::schema::type_info(), skipper<Iterator>> type_info_;
  qi::rule<Iterator, ast::schema::type(), skipper<Iterator>> type_;
  qi::rule<Iterator, ast::schema::enum_type(), skipper<Iterator>> enum_;
  qi::rule<Iterator, ast::schema::vector_type(), skipper<Iterator>> vector_;
  qi::rule<Iterator, ast::schema::set_type(), skipper<Iterator>> set_;
  qi::rule<Iterator, ast::schema::table_type(), skipper<Iterator>> table_;
  qi::rule<Iterator, ast::schema::record_type(), skipper<Iterator>> record_;
  qi::rule<Iterator, std::string()> identifier_;
};

} // namespace parser
} // namespace detail
} // namespace vast

#endif
