#ifndef VAST_CONCEPT_PARSEABLE_VAST_TYPE_H
#define VAST_CONCEPT_PARSEABLE_VAST_TYPE_H

#include "vast/type.h"

#include "vast/concept/parseable/core/list.h"
#include "vast/concept/parseable/core/operators.h"
#include "vast/concept/parseable/core/parser.h"
#include "vast/concept/parseable/numeric/integral.h"
#include "vast/concept/parseable/string/quoted_string.h"
#include "vast/concept/parseable/string/symbol_table.h"
#include "vast/concept/parseable/vast/identifier.h"

namespace vast {

class type_table : public parser<type_table> {
public:
  using attribute = type;

  type_table() = default;

  type_table(std::initializer_list<std::pair<const std::string, type>> init) {
    for (auto& pair : init)
      add(pair.first, pair.second);
  }

  bool add(std::string const& name, type t) {
    if (name.empty() || (name != t.name() && !t.name(name)))
      return false;
    symbols_.symbols.insert({name, t});
    return true;
  }

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    return symbols_.parse(f, l, a);
  }

private:
  symbol_table<type> symbols_;
};

/// Parses a type with the help of a symbol table.
struct type_parser : parser<type_parser> {
  using attribute = type;

  type_parser(type_table const* symbols = nullptr)
    : symbol_type{symbols} {
  }

  template <typename T>
  static type to_basic_type(std::vector<type::attribute> a) {
    return T{std::move(a)};
  };

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
    // Whitespace
    static auto ws = ignore(*parsers::space);
    // Attributes: type meta data
    static auto attr_key
      = "invalid"_p ->* [] { return type::attribute::invalid; }
      | "skip"_p    ->* [] { return type::attribute::skip; }
      | "default"_p ->* [] { return type::attribute::default_; }
      ;
    static auto to_attr =
      [](std::tuple<type::attribute::key_type, std::string> t) {
        return type::attribute{std::get<0>(t), std::get<1>(t)};
      };
    static auto attr
      = ('&' >> attr_key >> ~('=' >> parsers::qq_str)) ->* to_attr
      ;
    static auto attr_list = *(ws >> attr);
    // Basic types
    static auto basic_type
      = "bool" >> attr_list      ->* to_basic_type<type::boolean>
      | "int" >> attr_list       ->* to_basic_type<type::integer>
      | "count" >> attr_list     ->* to_basic_type<type::count>
      | "real" >> attr_list      ->* to_basic_type<type::real>
      | "time" >> attr_list      ->* to_basic_type<type::time_point>
      | "duration" >> attr_list  ->* to_basic_type<type::time_duration>
      | "string" >> attr_list    ->* to_basic_type<type::string>
      | "pattern" >> attr_list   ->* to_basic_type<type::pattern>
      | "addr" >> attr_list      ->* to_basic_type<type::address>
      | "subnet" >> attr_list    ->* to_basic_type<type::subnet>
      | "port" >> attr_list      ->* to_basic_type<type::port>
      ;
    // Enumeration
    using enum_tuple = std::tuple<
      std::vector<std::string>,
      std::vector<type::attribute>
    >;
    static auto to_enum = [](enum_tuple t) -> vast::type {
      return type::enumeration{std::get<0>(t), std::get<1>(t)};
    };
    static auto enum_type
      = ("enum" >> ws >> '{'
      >> (ws >> parsers::identifier) % ','
      >> ws >> '}' >> attr_list) ->* to_enum
      ;
    // Compound types
    rule<Iterator, type> type_type;
    // Vector
    using sequence_tuple = std::tuple<type, std::vector<type::attribute>>;
    static auto to_vector = [](sequence_tuple t) -> type {
      return type::vector{std::get<0>(t), std::get<1>(t)};
    };
    auto vector_type
      = ("vector" >> ws >> '<' >> ws >> type_type >> ws >> '>') ->* to_vector
      ;
    // Set
    static auto to_set = [](sequence_tuple t) -> type {
      return type::set{std::get<0>(t), std::get<1>(t)};
    };
    auto set_type
      = ("set" >> ws >> '<' >> ws >> type_type >> ws >> '>') ->* to_set
      ;
    // Table
    using table_tuple = std::tuple<type, type, std::vector<type::attribute>>;
    static auto to_table = [](table_tuple t) -> type {
      return type{type::table{std::get<0>(t), std::get<1>(t), std::get<2>(t)}};
    };
    auto table_type
      = ("table" >> ws >> '<' >> ws
      >> type_type >> ws >> ',' >> ws >> type_type >> ws
      >> '>' >> attr_list) ->* to_table;
      ;
    // Record
    using record_tuple = std::tuple<
      std::vector<type::record::field>,
      std::vector<type::attribute>
    >;
    static auto to_field = [](std::tuple<std::string, type> t) {
      return type::record::field{std::get<0>(t), std::get<1>(t)};
    };
    static auto to_record = [](record_tuple t) -> type {
      return type::record{std::get<0>(t), std::get<1>(t)};
    };
    auto field
      = (parsers::identifier >> ws >> ':' >> ws >> type_type) ->* to_field
      ;
    auto record_type
      = ("record" >> ws >> '{'
      >> (ws >> field) % ',' >> ws
      >> '}' >> attr_list) ->* to_record;
      ;
    // Complete type
    if (symbol_type)
      type_type
        = *symbol_type
        | basic_type
        | enum_type
        | vector_type
        | set_type
        | table_type
        | record_type
        ;
    else // As above, just without the symbol table.
      type_type
        = basic_type
        | enum_type
        | vector_type
        | set_type
        | table_type
        | record_type
        ;
    return type_type.parse(f, l, a);
  }

  type_table const* symbol_type;
};

template <>
struct parser_registry<type> {
  using type = type_parser;
};

namespace parsers {

auto const type = make_parser<vast::type>();

} // namespace parsers
} // namespace vast

#endif
