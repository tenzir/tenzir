//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/literal.hpp"

namespace vast {

namespace policy {

struct single_char_bool_policy {
  template <class Iterator>
  static bool parse_true(Iterator& f, const Iterator& l) {
    return parsers::ch<'T'>(f, l, unused);
  }

  template <class Iterator>
  static bool parse_false(Iterator& f, const Iterator& l) {
    return parsers::ch<'F'>(f, l, unused);
  }
};

struct zero_one_bool_policy {
  template <class Iterator>
  static bool parse_true(Iterator& f, const Iterator& l) {
    return parsers::ch<'1'>(f, l, unused);
  }

  template <class Iterator>
  static bool parse_false(Iterator& f, const Iterator& l) {
    return parsers::ch<'0'>(f, l, unused);
  }
};

struct literal_bool_policy {
  template <class Iterator>
  static bool parse_true(Iterator& f, const Iterator& l) {
    return parsers::lit{"true"}(f, l, unused);
  }

  template <class Iterator>
  static bool parse_false(Iterator& f, const Iterator& l) {
    return parsers::lit{"false"}(f, l, unused);
  }
};

} // namespace policy

template <class Policy>
struct bool_parser : parser_base<bool_parser<Policy>> {
  using attribute = bool;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l)
      return false;
    if (Policy::parse_true(f, l))
      a = true;
    else if (Policy::parse_false(f, l))
      a = false;
    else
      return false;
    return true;
  }
};

using single_char_bool_parser = bool_parser<policy::single_char_bool_policy>;
using zero_one_bool_parser = bool_parser<policy::zero_one_bool_policy>;
using literal_bool_parser = bool_parser<policy::literal_bool_policy>;

template <>
struct parser_registry<bool> {
  using type = single_char_bool_parser;
};

namespace parsers {

auto const tf = bool_parser<policy::single_char_bool_policy>{};
auto const zero_one = bool_parser<policy::zero_one_bool_policy>{};
auto const boolean = bool_parser<policy::literal_bool_policy>{};

} // namespace parsers
} // namespace vast
