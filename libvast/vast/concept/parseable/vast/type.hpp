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

#pragma once

#include "vast/type.hpp"

#include "vast/concept/parseable/core/list.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/parseable/string/symbol_table.hpp"
#include "vast/concept/parseable/vast/identifier.hpp"

namespace vast {

class type_table : public parser<type_table> {
public:
  using attribute = type;

  type_table() = default;

  type_table(std::initializer_list<std::pair<const std::string, type>> init) {
    for (auto& pair : init)
      add(pair.first, pair.second);
  }

  bool add(const std::string& name, type t) {
    if (name.empty() || name != t.name())
      return false;
    t = t.name(name);
    symbols_.symbols.insert({name, t});
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    return symbols_(f, l, a);
  }

private:
  symbol_table<type> symbols_;
};

/// Parses a type with the help of a symbol table.
struct type_parser : parser<type_parser> {
  using attribute = type;

  type_parser(const type_table* symbols = nullptr) : symbol_type{symbols} {
    // nop
  }

  template <class T>
  static type to_basic_type(std::vector<vast::attribute> a) {
    T b;
    return b.attributes(std::move(a));
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    // Whitespace
    static auto ws = ignore(*parsers::space);
    // Attributes: type meta data
    static auto to_attr =
      [](std::tuple<std::string, optional<std::string>> t) {
        return vast::attribute{std::get<0>(t), std::get<1>(t)};
      };
    static auto attr
      = ('&' >> parsers::identifier >> -('=' >> parsers::qq_str)) ->* to_attr
      ;
    static auto attr_list = *(ws >> attr);
    // Basic types
    static auto basic_type_parser
      = "bool" >> attr_list      ->* to_basic_type<boolean_type>
      | "int" >> attr_list       ->* to_basic_type<integer_type>
      | "count" >> attr_list     ->* to_basic_type<count_type>
      | "real" >> attr_list      ->* to_basic_type<real_type>
      | "duration" >> attr_list  ->* to_basic_type<timespan_type>
      | "time" >> attr_list      ->* to_basic_type<timestamp_type>
      | "string" >> attr_list    ->* to_basic_type<string_type>
      | "pattern" >> attr_list   ->* to_basic_type<pattern_type>
      | "addr" >> attr_list      ->* to_basic_type<address_type>
      | "subnet" >> attr_list    ->* to_basic_type<subnet_type>
      | "port" >> attr_list      ->* to_basic_type<port_type>
      ;
    // Enumeration
    using enum_tuple = std::tuple<
      std::vector<std::string>,
      std::vector<vast::attribute>
    >;
    static auto to_enum = [](enum_tuple t) -> type {
      return enumeration_type{std::get<0>(t)}.attributes(std::get<1>(t));
    };
    static auto enum_type_parser
      = ("enum" >> ws >> '{'
      >> (ws >> parsers::identifier) % ','
      >> ws >> '}' >> attr_list) ->* to_enum
      ;
    // Compound types
    rule<Iterator, type> type_type;
    // Vector
    using sequence_tuple = std::tuple<type, std::vector<vast::attribute>>;
    static auto to_vector = [](sequence_tuple t) -> type {
      return vector_type{std::get<0>(t)}.attributes(std::get<1>(t));
    };
    auto vector_type_parser
      = ("vector" >> ws >> '<' >> ws >> type_type >> ws >> '>') ->* to_vector
      ;
    // Set
    static auto to_set = [](sequence_tuple t) -> type {
      return set_type{std::get<0>(t)}.attributes(std::get<1>(t));
    };
    auto set_type_parser
      = ("set" >> ws >> '<' >> ws >> type_type >> ws >> '>') ->* to_set
      ;
    // Map
    using map_tuple = std::tuple<type, type, std::vector<vast::attribute>>;
    static auto to_map = [](map_tuple t) -> type {
      auto m = map_type{std::get<0>(t), std::get<1>(t)};
      return m.attributes(std::get<2>(t));
    };
    auto map_type_parser
      = ("map" >> ws >> '<' >> ws
      >> type_type >> ws >> ',' >> ws >> type_type >> ws
      >> '>' >> attr_list) ->* to_map;
      ;
    // Record
    using record_tuple = std::tuple<
      std::vector<record_field>,
      std::vector<vast::attribute>
    >;
    static auto to_field = [](std::tuple<std::string, type> t) {
      return record_field{std::get<0>(t), std::get<1>(t)};
    };
    static auto to_record = [](record_tuple t) -> type {
      return record_type{std::get<0>(t)}.attributes(std::get<1>(t));
    };
    auto field
      = (parsers::identifier >> ws >> ':' >> ws >> type_type) ->* to_field
      ;
    auto record_type_parser
      = ("record" >> ws >> '{'
      >> (ws >> field) % ',' >> ws
      >> '}' >> attr_list) ->* to_record;
      ;
    // Complete type
    if (symbol_type)
      type_type
        = *symbol_type
        | basic_type_parser
        | enum_type_parser
        | vector_type_parser
        | set_type_parser
        | map_type_parser
        | record_type_parser
        ;
    else // As above, just without the symbol table.
      type_type
        = basic_type_parser
        | enum_type_parser
        | vector_type_parser
        | set_type_parser
        | map_type_parser
        | record_type_parser
        ;
    return type_type(f, l, a);
  }

  const type_table* symbol_type;
};

template <>
struct parser_registry<type> {
  using type = type_parser;
};

namespace parsers {

auto const type = make_parser<vast::type>();

} // namespace parsers
} // namespace vast
