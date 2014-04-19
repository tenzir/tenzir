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

    basic_type_.add
        ("bool",      {"bool", ast::schema::bool_type})
        ("int",       {"int", ast::schema::int_type})
        ("count",     {"count", ast::schema::uint_type})
        ("double",    {"double", ast::schema::double_type})
        ("interval",  {"interval", ast::schema::time_frame_type})
        ("time",      {"time", ast::schema::time_point_type})
        ("string",    {"string", ast::schema::string_type})
        ("pattern",   {"pattern", ast::schema::regex_type})
        ("addr",      {"addr", ast::schema::address_type})
        ("subnet",    {"subnet", ast::schema::prefix_type})
        ("port",      {"port", ast::schema::port_type})
        ;

    auto add_type =
      [=](ast::schema::type_declaration const& decl,
          qi::unused_type, qi::unused_type)
      {
        auto& name = decl.name;
        if (basic_type_.find(name) || user_type_.find(name) || event_.find(name))
          throw std::runtime_error("duplicate type name: " + name);

        if (auto ti = boost::get<ast::schema::type_info>(&decl.type))
        {
          user_type_.add(name, {name, *ti});
        }
        else if (auto t = boost::get<ast::schema::type>(&decl.type))
        {
          user_type_.add(name, {name, t->info});
        }
      };

    auto add_event =
      [&](ast::schema::event_declaration const& ed,
          qi::unused_type, qi::unused_type)
      {
        if (event_.find(ed.name))
          throw std::runtime_error("duplicate event name: " + ed.name);
        else if (user_type_.find(ed.name))
          throw std::runtime_error("conflicting event name: " + ed.name);

        event_.add(ed.name, ed);
      };

    schema_
        =   *statement_
        ;

    statement_
        =   type_decl_   [_val = _1][add_type]
        |   event_decl_  [_val = _1][add_event]
        ;

    event_decl_
        =   lit("event")
        >   identifier_
        >   '('
        >   -(argument_ % ',')
        >   ')'
        ;

    type_decl_
        =   lit("type")
        >   identifier_
        >   ':'
        >   ( type_info_
            | user_type_
            | basic_type_
            )
        ;

    argument_
        =   identifier_
        >   ':'
        >   type_
        >   -(+attribute_)
        ;

    attribute_
        =  lexeme
           [
                '&'
            >   identifier_
            >   -(  '='
                 >  (( '"'
                       > *(char_ - '"')
                       > '"' ) // Extra parentheses removes a compiler warning.
                    |  +(char_ - space)
                    )
                 )
           ]
        ;

    type_info_
        =   enum_
        |   vector_
        |   set_
        |   table_
        |   record_
        ;

    type_
        =   user_type_    [_val = _1]
        |   type_info_    [at_c<1>(_val) = _1]
        |   basic_type_   [_val = _1]
        ;

    enum_
        =   lit("enum")
        >   '{'
        >   identifier_ % ','
        >   '}'
        ;

    vector_
        =   lit("vector")
        >   "of"
        >   type_
        ;

    set_
        =   lit("set")
        >   '['
        >   type_
        >   ']'
        ;

    table_
        =   lit("table")
        >   '['
        >   type_
        >   ']'
        >   "of"
        >   type_
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

    event_.name("event symbol");
    type_.name("type symbol");
    schema_.name("schema");
    statement_.name("statement");
    type_decl_.name("type declaration");
    event_decl_.name("event declaration");
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

  qi::symbols<char, ast::schema::event_declaration> event_;
  qi::symbols<char, ast::schema::type> basic_type_;
  qi::symbols<char, ast::schema::type> user_type_;
  qi::rule<Iterator, ast::schema::schema(), skipper<Iterator>> schema_;
  qi::rule<Iterator, ast::schema::statement(), skipper<Iterator>> statement_;
  qi::rule<Iterator, ast::schema::type_declaration(), skipper<Iterator>> type_decl_;
  qi::rule<Iterator, ast::schema::event_declaration(), skipper<Iterator>> event_decl_;
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
