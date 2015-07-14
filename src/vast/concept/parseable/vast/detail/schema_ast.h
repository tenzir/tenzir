#ifndef VAST_DETAIL_AST_SCHEMA_H
#define VAST_DETAIL_AST_SCHEMA_H

#include <vector>
#include <string>

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

using type_info = boost::variant<
  basic_type,
  enum_type,
  boost::recursive_wrapper<vector_type>,
  boost::recursive_wrapper<set_type>,
  boost::recursive_wrapper<table_type>,
  boost::recursive_wrapper<record_type>,
  std::string
>;

struct attribute
{
  std::string key;
  boost::optional<std::string> value;
};

struct type
{
  type() = default;

  type(type_info i, std::vector<attribute> a = {})
    : info{std::move(i)},
      attrs{std::move(a)}
  {
  }

  type_info info;
  std::vector<attribute> attrs;
};

struct vector_type
{
  type element_type;
};

struct set_type
{
  type element_type;
};

struct table_type
{
  type key_type;
  type value_type;
};

struct argument_declaration
{
  std::string name;
  schema::type type;
};

struct record_type
{
  std::vector<argument_declaration> args;
};

struct type_declaration
{
  std::string name;
  schema::type type;
};

using schema = std::vector<type_declaration>;

} // namespace schema
} // namespace ast
} // namespace detail
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::attribute,
    (std::string, key)
    (boost::optional<std::string>, value))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::type,
    (vast::detail::ast::schema::type_info, info)
    (std::vector<vast::detail::ast::schema::attribute>, attrs))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::enum_type,
    (std::vector<std::string>, fields))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::vector_type,
    (vast::detail::ast::schema::type, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::set_type,
    (vast::detail::ast::schema::type, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::table_type,
    (vast::detail::ast::schema::type, key_type)
    (vast::detail::ast::schema::type, value_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::argument_declaration,
    (std::string, name)
    (vast::detail::ast::schema::type, type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::record_type,
    (std::vector<vast::detail::ast::schema::argument_declaration>, args))

BOOST_FUSION_ADAPT_STRUCT(
    vast::detail::ast::schema::type_declaration,
    (std::string, name)
    (vast::detail::ast::schema::type, type))

#endif
