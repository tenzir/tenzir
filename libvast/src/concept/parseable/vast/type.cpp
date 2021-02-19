/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/concept/parseable/vast/type.hpp"

#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/rule.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"

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
    [](std::tuple<std::string, optional<std::string>> xs) {
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
    ( "bool"_p      ->* type_factory<bool_type>
    | "int"_p       ->* type_factory<integer_type>
    | "count"_p     ->* type_factory<count_type>
    | "real"_p      ->* type_factory<real_type>
    | "duration"_p  ->* type_factory<duration_type>
    | "time"_p      ->* type_factory<time_type>
    | "string"_p    ->* type_factory<string_type>
    | "pattern"_p   ->* type_factory<pattern_type>
    | "addr"_p      ->* type_factory<address_type>
    | "subnet"_p    ->* type_factory<subnet_type>
    ) >> &(!parsers::identifier_char)
    ;
  // Enumeration
  static auto to_enum = [](std::vector<std::string> fields) -> type {
    return enumeration_type{std::move(fields)};
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
    return list_type{std::move(xs)};
  };
  auto list_type_parser
    = ("list" >> skp >> '<' >> skp >> ref(type_type) >> skp >> '>')
      ->* to_list
    ;
  // Map
  using map_tuple = std::tuple<type, type>;
  static auto to_map = [](map_tuple xs) -> type {
    auto& [key_type, value_type] = xs;
    return map_type{std::move(key_type), std::move(value_type)};
  };
  auto map_type_parser
    = ("map" >> skp >> '<' >> skp
    >> vast::ref(type_type) >> skp >> ',' >> skp >> ref(type_type) >> skp
    >> '>') ->* to_map;
    ;
  // Record
  static auto to_field = [](std::tuple<std::string, type> xs) {
    auto& [field_name, field_type] = xs;
    return record_field{std::move(field_name), std::move(field_type)};
  };
  static auto to_record = [](std::vector<record_field> fields) -> type {
    return record_type{std::move(fields)};
  };
  auto field
    = ((parsers::identifier | parsers::qqstr) >> skp >> ':' >> skp
    >> ref(type_type))
    ->* to_field
    ;
  auto record_type_parser
    = ("record" >> skp >> '{'
    >> ((skp >> field >> skp) % ',') >> ~(',' >> skp)
    >> '}') ->* to_record;
    ;
  static auto to_named_none_type = [](std::string name) -> type {
    return none_type{}.name(std::move(name));
  };
  static auto placeholder_parser
    = (parsers::identifier) ->* to_named_none_type
    ;
  // Complete type
  using type_tuple = std::tuple<
    vast::type,
    std::vector<vast::attribute>
  >;
  static auto insert_attributes = [](type_tuple xs) {
    auto& [t, attrs] = xs;
    return t.attributes(std::move(attrs));
  };
  type_type = (
    ( basic_type_parser
    | enum_type_parser
    | list_type_parser
    | map_type_parser
    | record_type_parser
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
