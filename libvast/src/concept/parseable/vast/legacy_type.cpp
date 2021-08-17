//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/legacy_type.hpp"

#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"

#include <caf/optional.hpp>

namespace vast {

template <class T>
static type type_factory() {
  return T{};
}

template <class Iterator, class Attribute>
bool type_parser::parse(Iterator& f, const Iterator& l, Attribute& a) const {
  // clang-format off
  // Attributes: type meta data
  static auto to_attr =
    [](std::tuple<std::string, caf::optional<std::string>> xs) {
      auto& [key, value] = xs;
      return vast::attribute{std::move(key), std::move(value)};
    };
  static constexpr auto attr_value
    = parsers::qqstr
    | +(parsers::printable - (parsers::space | ',' | '>' | '}' ));
  static auto attr
    = ('#' >> parsers::identifier >> -('=' >> attr_value)) ->* to_attr;
  static auto attr_list = *(skp >> attr);
  // Basic types
  using namespace parser_literals;
  static auto basic_type_parser
    =
    ( "bool"_p      ->* type_factory<legacy_bool_type>
    | "int"_p       ->* type_factory<legacy_integer_type>
    | "count"_p     ->* type_factory<legacy_count_type>
    | "real"_p      ->* type_factory<legacy_real_type>
    | "duration"_p  ->* type_factory<legacy_duration_type>
    | "time"_p      ->* type_factory<legacy_time_type>
    | "string"_p    ->* type_factory<legacy_string_type>
    | "pattern"_p   ->* type_factory<legacy_pattern_type>
    | "addr"_p      ->* type_factory<legacy_address_type>
    | "subnet"_p    ->* type_factory<legacy_subnet_type>
    ) >> &(!parsers::identifier_char)
    ;
  // Enumeration
  static auto to_enum = [](std::vector<std::string> fields) -> type {
    return legacy_enumeration_type{std::move(fields)};
  };
  static auto enum_type_parser
    = ("enum" >> skp >> '{'
    >> ((skp >> parsers::identifier >> skp) % ',') >> ~(',' >> skp)
    >> '}') ->* to_enum
    ;
  // Compound types
  rule<Iterator, type> type_type;
  // List
  static auto to_list = [](type xs) -> type {
    return legacy_list_type{std::move(xs)};
  };
  auto legacy_list_type_parser
    = ("list" >> skp >> '<' >> skp >> ref(type_type) >> skp >> '>')
      ->* to_list
    ;
  // Map
  using map_tuple = std::tuple<type, type>;
  static auto to_map = [](map_tuple xs) -> type {
    auto& [key_type, value_type] = xs;
    return legacy_map_type{std::move(key_type), std::move(value_type)};
  };
  auto legacy_map_type_parser
    = ("map" >> skp >> '<' >> skp
    >> vast::ref(type_type) >> skp >> ',' >> skp >> ref(type_type) >> skp
    >> '>') ->* to_map
    ;
  // Record
  static auto to_field = [](std::tuple<std::string, type> xs) {
    auto& [field_name, field_type] = xs;
    return record_field{std::move(field_name), std::move(field_type)};
  };
  static auto to_record = [](std::vector<record_field> fields) -> type {
    return legacy_record_type{std::move(fields)};
  };
  auto field_name = parsers::identifier | parsers::qqstr;
  auto field = (field_name >> skp >> ':' >> skp >> ref(type_type)) ->* to_field;
  auto legacy_record_type_parser
    = ("record" >> skp >> '{'
    >> ((skp >> field >> skp) % ',') >> ~(',' >> skp)
    >> '}') ->* to_record
    ;
  static auto to_named_legacy_none_type = [](std::string name) -> type {
    return legacy_none_type{}.name(std::move(name));
  };
  static auto placeholder_parser
    = (parsers::identifier) ->* to_named_legacy_none_type
    ;
  rule<Iterator, type> type_expr_parser;
  auto algebra_leaf_parser
    = legacy_record_type_parser
    | placeholder_parser
    ;
  auto algebra_operand_parser
    = algebra_leaf_parser
    | ref(type_expr_parser)
    ;
  auto rplus_parser = "+>" >> skp >> algebra_operand_parser ->* [](type t) {
    return record_field{"+>", std::move(t)};
  };
  auto plus_parser = '+' >> skp >> algebra_operand_parser ->* [](type t) {
    return record_field{"+", std::move(t)};
  };
  auto lplus_parser = "<+" >> skp >> algebra_operand_parser ->* [](type t) {
    return record_field{"<+", std::move(t)};
  };
  auto to_minus_record = [](std::vector<std::string> path) {
    legacy_record_type result;
    for (auto& key : path)
      result.fields.emplace_back(std::move(key), legacy_bool_type{});
    return record_field{"-", std::move(result)};
  };
  // Keep in sync with parsers::identifier.
  auto qualified_field_name
    = ((+(parsers::alnum | parsers::ch<'_'>) | parsers::qqstr) % '.');
  auto minus_parser = '-' >> skp >> qualified_field_name ->* to_minus_record;
  auto algebra_parser
    = rplus_parser
    | plus_parser
    | lplus_parser
    | minus_parser
    ;
  type_expr_parser = (algebra_operand_parser >> skp >> (+(skp >> algebra_parser)))
    ->* [](std::tuple<type, std::vector<record_field>> xs) -> type {
      auto& [lhs, op_operands] = xs;
      legacy_record_type result;
      result.fields = {record_field{"", std::move(lhs)}};
      result.fields.insert(
        result.fields.end(),
        op_operands.begin(), op_operands.end());
      return result.attributes({{"$algebra"}});
    };
  // Complete type
  using type_tuple = std::tuple<
    vast::type,
    std::vector<vast::attribute>
  >;
  static auto insert_attributes = [](type_tuple xs) {
    auto& [t, attrs] = xs;
    return t.update_attributes(std::move(attrs));
  };
  type_type = (
    ( type_expr_parser
    | basic_type_parser
    | enum_type_parser
    | legacy_list_type_parser
    | legacy_map_type_parser
    | legacy_record_type_parser
    | placeholder_parser
    ) >> attr_list) ->* insert_attributes
    ;
  return type_type(f, l, a);
  // clang-format on
}

template bool
type_parser::parse(std::string::iterator&, const std::string::iterator&,
                   unused_type&) const;
template bool type_parser::parse(std::string::iterator&,
                                 const std::string::iterator&, type&) const;

template bool
type_parser::parse(std::string::const_iterator&,
                   const std::string::const_iterator&, unused_type&) const;
template bool
type_parser::parse(std::string::const_iterator&,
                   const std::string::const_iterator&, type&) const;

template bool
type_parser::parse(char const*&, char const* const&, unused_type&) const;
template bool type_parser::parse(char const*&, char const* const&, type&) const;

} // namespace vast
