//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/parseable/core/rule.hpp"
#include "tenzir/concept/parseable/numeric.hpp"
#include "tenzir/concept/parseable/string/quoted_string.hpp"
#include "tenzir/concept/parseable/string/string.hpp"
#include "tenzir/concept/parseable/tenzir/identifier.hpp"
#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/tenzir/pattern.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/concept/parseable/tenzir/subnet.hpp"
#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/data.hpp"

#include <caf/none.hpp>

namespace tenzir {

struct null_parser : parser_base<null_parser> {
  using attribute = caf::none_t;

  template <class Iterator, class Attribute>
  auto parse(Iterator& f, const Iterator& l, Attribute&) const -> bool {
    return parsers::lit{"null"}.parse(f, l, unused);
  }
};

namespace parsers {

constexpr inline auto number = (parsers::count >> &!chr{'.'})
                               | (parsers::integer >> &!chr{'.'})
                               | parsers::real;

constexpr inline auto null = null_parser{};

} // namespace parsers

struct simple_data_parser : parser_base<simple_data_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    static auto p = parsers::net | parsers::ip | parsers::time
                    | parsers::duration | parsers::number | parsers::boolean;
    return p(f, l, a);
  }
};

struct data_parser : parser_base<data_parser> {
  using attribute = data;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    static auto p = make<Iterator>();
    return p(f, l, a);
  }

private:
  template <class Iterator>
  static auto make() {
    using namespace parser_literals;
    rule<Iterator, data> p;
    auto ws = ignore(*parsers::space);
    auto x = ws >> ref(p) >> ws;
    auto kvp = x >> "->" >> x;
    auto trailing_comma = ~(',' >> ws);
    auto named_field = ws >> parsers::identifier >> ":" >> x;
    // clang-format off
    auto unnamed_field = x ->* [](data value) {
      return std::make_pair(std::string{}, std::move(value));
    };
    // A record can either be ordered with unnamed fields or unordered
    // with named fields. Allowing a mixture of both would mean we'd
    // have to deal with ambiguous inputs.
    auto record_parser =
        '<' >> ~parse_as<record>(named_field % ',') >> trailing_comma >> '>'
        // Creating a record with repeated field names technically violates
        // the consistency of the underlying stable_map. We live with that
        // until record is refactored into a proper type (FIXME).
      | ('<' >> (unnamed_field % ',') >> trailing_comma >> '>')
        ->* [](record::vector_type&& xs) {
          return record::make_unsafe(std::move(xs));
        };
    // Order matters here: If X is a prefix of Y, then X must come after Y.
    // For example `3d` is a prefix of `3d::`, hence duration must be after IP.
    p = parsers::net
      | parsers::ip
      | parsers::time
      | parsers::duration
      | parsers::number
      | parsers::boolean
      | parsers::qqstr
      | parsers::pattern
      | '[' >> ~(x % ',') >> trailing_comma >> ']'
      | '{' >> ~parse_as<map>(kvp % ',') >> trailing_comma >> '}'
      | record_parser
      // TODO: We have two representations for the null type: `null` and `_`,
      // but should consider dropping `_` eventually.
      | parsers::null
      | parse_as<caf::none_t>(parsers::ch<'_'>)
      ;
    // clang-format on
    return p;
  }
};

template <>
struct parser_registry<data> {
  using type = data_parser;
};

template <>
struct parser_registry<caf::none_t> {
  using type = null_parser;
};

namespace parsers {

constexpr inline auto simple_data = simple_data_parser{};
constexpr inline auto data = data_parser{};

} // namespace parsers
} // namespace tenzir
