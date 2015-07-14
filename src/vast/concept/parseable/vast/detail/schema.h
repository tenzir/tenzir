#ifndef VAST_CONCEPT_PARSEABLE_VAST_DETAIL_SCHEMA_H
#define VAST_CONCEPT_PARSEABLE_VAST_DETAIL_SCHEMA_H

#include "vast/schema.h"
#include "vast/type.h"
#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/vast/detail/schema_ast.h"
#include "vast/concept/parseable/vast/detail/error_handler.h"
#include "vast/concept/parseable/vast/detail/skipper.h"

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
      [=](std::string const& id, unused_type, unused_type)
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

inline std::vector<type::attribute> make_attrs(
    std::vector<ast::schema::attribute> const& attrs)
{
  std::vector<type::attribute> r;
  for (auto& a : attrs)
  {
    auto key = type::attribute::invalid;
    if (a.key == "skip")
      key = type::attribute::skip;
    else if (a.key == "default")
      key = type::attribute::default_;
    std::string value;
    if (a.value)
      value = *a.value;
    r.emplace_back(key, std::move(value));
  }
  return r;
}

class type_factory
{
public:
  using result_type = trial<type>;

  type_factory(vast::schema const& s, std::vector<ast::schema::attribute> const& as)
    : schema_{s},
      attrs_{make_attrs(as)}
  {
  }

  trial<type> operator()(std::string const& type_name) const
  {
    if (auto x = schema_.find_type(type_name))
      return *x;
    else
      return error{"unknown type: ", type_name};
  }

  trial<type> operator()(ast::schema::basic_type bt) const
  {
    switch (bt)
    {
      default:
        return error{"missing type implementation"};
      case ast::schema::bool_type:
        return {type::boolean{attrs_}};
      case ast::schema::int_type:
        return {type::integer{attrs_}};
      case ast::schema::uint_type:
        return {type::count{attrs_}};
      case ast::schema::double_type:
        return {type::real{attrs_}};
      case ast::schema::time_point_type:
        return {type::time_point{attrs_}};
      case ast::schema::time_frame_type:
        return {type::time_duration{attrs_}};
      case ast::schema::string_type:
        return {type::string{attrs_}};
      case ast::schema::regex_type:
        return {type::pattern{attrs_}};
      case ast::schema::address_type:
        return {type::address{attrs_}};
      case ast::schema::prefix_type:
        return {type::subnet{attrs_}};
      case ast::schema::port_type:
        return {type::port{attrs_}};
    }
  }

  trial<type> operator()(ast::schema::enum_type const& t) const
  {
    return {type::enumeration{t.fields, attrs_}};
  }

  trial<type> operator()(ast::schema::vector_type const& t) const
  {
    auto elem = make_type(t.element_type);
    if (! elem)
      return elem;
    return {type::vector{std::move(*elem), attrs_}};
  }

  trial<type> operator()(ast::schema::set_type const& t) const
  {
    auto elem = make_type(t.element_type);
    if (! elem)
      return elem;
    return {type::set{std::move(*elem), attrs_}};
  }

  trial<type> operator()(ast::schema::table_type const& t) const
  {
    auto k = make_type(t.key_type);
    if (! k)
      return k;
    auto v = make_type(t.value_type);
    if (! v)
      return v;
    return {type::table{std::move(*k), std::move(*v), attrs_}};
  }

  trial<type> operator()(ast::schema::record_type const& t) const
  {
    std::vector<type::record::field> fields;
    for (auto& arg : t.args)
    {
      auto arg_type = make_type(arg.type);
      if (! arg_type)
        return arg_type;
      fields.push_back({arg.name, std::move(*arg_type)});
    }

    return {type::record{std::move(fields), attrs_}};
  }

  trial<type> make_type(ast::schema::type const& t) const
  {
    return boost::apply_visitor(type_factory{schema_, t.attrs}, t.info);
  }

private:
  vast::schema const& schema_;
  std::vector<type::attribute> attrs_;
};

struct schema_parser : vast::parser<schema_parser>
{
  using attribute = vast::schema;

  template <typename Iterator>
  bool parse(Iterator& f, Iterator const& l, vast::schema& sch) const
  {
    std::string err;
    detail::parser::error_handler<Iterator> on_error{f, l, err};
    detail::parser::schema<Iterator> grammar{on_error};
    detail::parser::skipper<Iterator> skipper;
    ast::schema::schema ast;
    if (! phrase_parse(f, l, grammar, skipper, ast))
      return false;
    sch.clear();
    for (auto& type_decl : ast)
    {
      // If we have a top-level identifier, we're dealing with a type alias.
      // Everywhere else (e.g., inside records or table types), and identifier
      // will be resolved to the corresponding type.
      if (auto id = boost::get<std::string>(&type_decl.type.info))
      {
        auto t = sch.find_type(*id);
        if (! t)
          return false;
        auto a = type::alias{*t, make_attrs(type_decl.type.attrs)};
        a.name(type_decl.name);
        if (! sch.add(std::move(a)))
          return false;
      }
      auto t = boost::apply_visitor(
          type_factory{sch, type_decl.type.attrs}, type_decl.type.info);
      if (! t)
        return false;
      t->name(type_decl.name);
      if (! sch.add(std::move(*t)))
        return false;
    }
    return true;
  }
};

} // namespace detail
} // namespace vast

#endif
