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
static type to_basic_type(std::vector<vast::attribute> a) {
  T b;
  return b.attributes(std::move(a));
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
  static auto basic_type_parser
    = "bool" >> attr_list      ->* to_basic_type<bool_type>
    | "int" >> attr_list       ->* to_basic_type<integer_type>
    | "count" >> attr_list     ->* to_basic_type<count_type>
    | "real" >> attr_list      ->* to_basic_type<real_type>
    | "duration" >> attr_list  ->* to_basic_type<duration_type>
    | "time" >> attr_list      ->* to_basic_type<time_type>
    | "string" >> attr_list    ->* to_basic_type<string_type>
    | "pattern" >> attr_list   ->* to_basic_type<pattern_type>
    | "addr" >> attr_list      ->* to_basic_type<address_type>
    | "subnet" >> attr_list    ->* to_basic_type<subnet_type>
    ;
  // Enumeration
  using enum_tuple = std::tuple<
    std::vector<std::string>,
    std::vector<vast::attribute>
  >;
  static auto to_enum = [](enum_tuple xs) -> type {
    auto& [fields, attrs] = xs;
    return enumeration_type{std::move(fields)}.attributes(std::move(attrs));
  };
  static auto enum_type_parser
    = ("enum" >> skp >> '{'
    >> ((skp >> parsers::identifier >> skp) % ',') >> ~(',' >> skp)
    >> '}' >> attr_list) ->* to_enum
    ;
  // Compound types
  rule<Iterator, type> type_type;
  // List
  using sequence_tuple = std::tuple<type, std::vector<vast::attribute>>;
  static auto to_list = [](sequence_tuple xs) -> type {
    auto& [value_type, attrs] = xs;
    return list_type{std::move(value_type)}.attributes(std::move(attrs));
  };
  auto list_type_parser
    = ("list" >> skp >> '<' >> skp >> ref(type_type) >> skp >> '>')
      ->* to_list
    ;
  // Map
  using map_tuple = std::tuple<type, type, std::vector<vast::attribute>>;
  static auto to_map = [](map_tuple xs) -> type {
    auto& [key_type, value_type, attrs] = xs;
    auto m = map_type{std::move(key_type), std::move(value_type)};
    return m.attributes(std::move(attrs));
  };
  auto map_type_parser
    = ("map" >> skp >> '<' >> skp
    >> vast::ref(type_type) >> skp >> ',' >> skp >> ref(type_type) >> skp
    >> '>' >> attr_list) ->* to_map;
    ;
  // Record
  using record_tuple = std::tuple<
    std::vector<record_field>,
    std::vector<vast::attribute>
  >;
  static auto to_field = [](std::tuple<std::string, type> xs) {
    auto& [field_name, field_type] = xs;
    return record_field{std::move(field_name), std::move(field_type)};
  };
  static auto to_record = [](record_tuple xs) -> type {
    auto& [fields, attrs] = xs;
    return record_type{std::move(fields)}.attributes(std::move(attrs));
  };
  auto field
    = ((parsers::identifier | parsers::qqstr) >> skp >> ':' >> skp
    >> ref(type_type))
    ->* to_field
    ;
  auto record_type_parser
    = ("record" >> skp >> '{'
    >> ((skp >> field >> skp) % ',') >> ~(',' >> skp)
    >> '}' >> attr_list) ->* to_record;
    ;
  using none_tuple = std::tuple<std::string, std::vector<vast::attribute>>;
  static auto to_named_none_type = [](none_tuple xs) {
    auto& [n, a] = xs;
    return none_type{}.name(std::move(n)).attributes(std::move(a));
  };
  static auto placeholder_parser
    = (id >> attr_list) ->* to_named_none_type
    ;
  // Complete type
  type_type
    = basic_type_parser
    | enum_type_parser
    | list_type_parser
    | map_type_parser
    | record_type_parser
    | placeholder_parser
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
