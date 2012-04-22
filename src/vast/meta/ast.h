#ifndef VAST_META_AST_H
#define VAST_META_AST_H

#include <string>
#include <vector>
#include <boost/fusion/include/adapt_struct.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>

namespace vast {
namespace meta {
namespace ast {

enum basic_type
{
    bool_type,
    int_type,
    uint_type,
    double_type,
    duration_type,
    timepoint_type,
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
> type;

//struct identifier
//{
//    identifier(std::string const& s = "")
//      : name(s)
//    {
//    }
//
//    std::string name;
//};

typedef boost::variant<
    std::string   // Symbol table entry
  , type
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

typedef boost::variant<
    type_declaration
  , event_declaration
> statement;

typedef std::vector<statement> taxonomy;

} // namespace ast
} // namespace meta
} // namespace vast

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::enum_type,
    (std::vector<std::string>, fields))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::vector_type,
    (vast::meta::ast::type_info, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::set_type,
    (vast::meta::ast::type_info, element_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::table_type,
    (vast::meta::ast::type_info, key_type)
    (vast::meta::ast::type_info, value_type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::attribute,
    (std::string, key)
    (boost::optional<std::string>, value))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::argument_declaration,
    (std::string, name)
    (vast::meta::ast::type_info, type)
    (boost::optional<std::vector<vast::meta::ast::attribute>>, attrs))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::record_type,
    (std::vector<vast::meta::ast::argument_declaration>, args))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::type_declaration,
    (std::string, name)
    (vast::meta::ast::type_info, type))

BOOST_FUSION_ADAPT_STRUCT(
    vast::meta::ast::event_declaration,
    (std::string, name)
    (boost::optional<std::vector<vast::meta::ast::argument_declaration>>,
     args))

#endif
