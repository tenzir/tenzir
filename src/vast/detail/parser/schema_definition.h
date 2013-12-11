#ifndef VAST_DETAIL_PARSER_SCHEMA_DEFINITION_H
#define VAST_DETAIL_PARSER_SCHEMA_DEFINITION_H

#include "vast/detail/parser/boost.h"
#include "vast/detail/parser/schema.h"

namespace vast {
namespace detail {
namespace parser {

template <typename Iterator>
schema<Iterator>::schema(error_handler<Iterator>& on_error)
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
    [=](ast::schema::type_declaration const& decl, qi::unused_type, qi::unused_type)
    {
      auto& name = decl.name;
      if (basic_type_.find(name) || type_.find(name) || event_.find(name))
        throw std::runtime_error("conflicting type name");

      if (auto p = boost::get<ast::schema::type_type>(&decl.type))
      {
        type_.add(name, {name, *p});
      }
      else if (auto p = boost::get<ast::schema::type_info>(&decl.type))
      {
        type_.add(name, {name, p->type});
      }
    };

  auto add_event =
    [&](ast::schema::event_declaration const& ed, qi::unused_type, qi::unused_type)
    {
      if (event_.find(ed.name))
        throw std::runtime_error("duplicate event name");
      else if (type_.find(ed.name))
        throw std::runtime_error("conflicting event name");

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
      >   ( type_type_
          | type_
          | basic_type_
          )
      ;

  argument_
      =   identifier_
      >   ':'
      >   type_info_
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

  type_type_
      =   enum_
      |   vector_
      |   set_
      |   table_
      |   record_
      ;

  type_info_
      =   type_         [_val = _1]
      |   type_type_    [at_c<0>(_val) = "<anonymous>"][at_c<1>(_val) = _1]
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
      >   type_info_
      ;

  set_
      =   lit("set")
      >   '['
      >   type_info_
      >   ']'
      ;

  table_
      =   lit("table")
      >   '['
      >   type_info_
      >   ']'
      >   "of"
      >   type_info_
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
  type_type_.name("type");
  enum_.name("enum type");
  vector_.name("vector type");
  set_.name("set type");
  table_.name("table type");
  type_info_.name("type");
  record_.name("record type");
  identifier_.name("identifier");
}

} // namespace parser
} // namespace detail
} // namespace vast

#endif
