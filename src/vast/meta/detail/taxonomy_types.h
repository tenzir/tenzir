#ifndef VAST_META_DETAIL_TAXONOMY_TYPES_H
#define VAST_META_DETAIL_TAXONOMY_TYPES_H

#include <string>
#include <vector>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>

namespace vast {
namespace meta {
namespace detail {

struct vector_type;
struct set_type;
struct table_type;
struct record_type;

struct unknown_type {};
struct addr_type {};
struct bool_type {};
struct count_type {};
struct double_type {};
struct int_type {};
struct interval_type {};
struct file_type {};
struct port_type {};
struct string_type {};
struct subnet_type {};
struct time_type {};
struct enum_type
{
    std::vector<std::string> fields;
};

typedef boost::variant<
    unknown_type
  , addr_type
  , bool_type
  , count_type
  , double_type
  , interval_type
  , int_type
  , file_type
  , port_type
  , string_type
  , subnet_type
  , time_type
  , enum_type
  , boost::recursive_wrapper<vector_type>
  , boost::recursive_wrapper<set_type>
  , boost::recursive_wrapper<table_type>
  , boost::recursive_wrapper<record_type>
> plain_type;

typedef boost::variant<
    std::string   // Symbol table entry
  , plain_type
> type_info;

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
    std::string name;
    type_info type;
};

struct event_declaration
{
    std::string name;
    boost::optional<std::vector<argument_declaration>> args;
};

typedef boost::variant<type_declaration, event_declaration> statement;
typedef std::vector<statement> ast;

} // namespace detail
} // namespace meta
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::enum_type,
    (std::vector<std::string>, fields)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::vector_type,
    (vast::meta::detail::type_info, element_type)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::set_type,
    (vast::meta::detail::type_info, element_type)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::table_type,
    (vast::meta::detail::type_info, key_type)
    (vast::meta::detail::type_info, value_type)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::attribute,
    (std::string, key)
    (boost::optional<std::string>, value)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::argument_declaration,
    (std::string, name)
    (vast::meta::detail::type_info, type)
    (boost::optional<std::vector<vast::meta::detail::attribute>>, attrs)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::record_type,
    (std::vector<vast::meta::detail::argument_declaration>, args)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::type_declaration,
    (std::string, name)
    (vast::meta::detail::type_info, type)
)

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::detail::event_declaration,
    (std::string, name)
    (boost::optional<std::vector<vast::meta::detail::argument_declaration>>,
     args)
)

#endif
