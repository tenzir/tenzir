#ifndef VAST_DETAIL_AST_SCHEMA_H
#define VAST_DETAIL_AST_SCHEMA_H

#include <boost/optional.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <boost/fusion/include/adapt_struct.hpp>

namespace vast {
namespace detail {
namespace ast {
namespace schema {

enum basic_type
{
  bool_type,
  int_type,
  uint_type,
  double_type,
  time_frame_type,
  time_point_type,
  string_type,
  regex_type,
  address_type,
  prefix_type,
  port_type
};

struct enum_type
{
  std::vector<std::string> fields;
};

struct vector_type;
struct set_type;
struct table_type;
struct record_type;

typedef boost::variant<
    basic_type
  , enum_type
  , boost::recursive_wrapper<vector_type>
  , boost::recursive_wrapper<set_type>
  , boost::recursive_wrapper<table_type>
  , boost::recursive_wrapper<record_type>
> type_type;

struct type_info
{
  std::string name;
  type_type type;
};

struct vector_type
{
  type_info element_type;
};

struct set_type
{
  type_info element_type;
};

struct table_type
{
  type_info key_type;
  type_info value_type;
};

struct attribute
{
  std::string key;
  boost::optional<std::string> value;
};

struct argument_declaration
{
  std::string name;
  type_info type;
  boost::optional<std::vector<attribute>> attrs;
};

struct record_type
{
  std::vector<argument_declaration> args;
};

struct type_declaration
{
  typedef boost::variant<type_type, type_info> variant_type;
  std::string name;
  variant_type type;
};

struct event_declaration
{
  std::string name;
  boost::optional<std::vector<argument_declaration>> args;
};

typedef boost::variant<
    type_declaration
  , event_declaration
> statement;

typedef std::vector<statement> schema;

} // namespace schema
} // namespace ast
} // namespace detail
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::type_info,
    (std::string, name)
    (vast::detail::ast::schema::type_type, type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::enum_type,
    (std::vector<std::string>, fields))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::vector_type,
    (vast::detail::ast::schema::type_info, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::set_type,
    (vast::detail::ast::schema::type_info, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::table_type,
    (vast::detail::ast::schema::type_info, key_type)
    (vast::detail::ast::schema::type_info, value_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::attribute,
    (std::string, key)
    (boost::optional<std::string>, value))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::argument_declaration,
    (std::string, name)
    (vast::detail::ast::schema::type_info, type)
    (boost::optional<std::vector<vast::detail::ast::schema::attribute>>, attrs))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::record_type,
    (std::vector<vast::detail::ast::schema::argument_declaration>, args))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::type_declaration,
    (std::string, name)
    (vast::detail::ast::schema::type_declaration::variant_type, type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::event_declaration,
    (std::string, name)
    (boost::optional<std::vector<vast::detail::ast::schema::argument_declaration>>,
     args))


#endif
