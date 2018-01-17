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

#ifndef VAST_CONCEPT_PARSEABLE_NUMERIC_BOOL_HPP
#define VAST_CONCEPT_PARSEABLE_NUMERIC_BOOL_HPP

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/c_string.hpp"

namespace vast {

namespace detail {

struct single_char_bool_policy {
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l) {
    return char_parser{'T'}(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l) {
    return char_parser{'F'}(f, l, unused);
  }
};

struct zero_one_bool_policy {
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l) {
    return char_parser{'1'}(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l) {
    return char_parser{'0'}(f, l, unused);
  }
};

struct literal_bool_policy {
  template <typename Iterator>
  static bool parse_true(Iterator& f, Iterator const& l) {
    return c_string_parser{"true"}(f, l, unused);
  }

  template <typename Iterator>
  static bool parse_false(Iterator& f, Iterator const& l) {
    return c_string_parser{"false"}(f, l, unused);
  }
};

} // namespace detail

template <typename Policy>
struct bool_parser : parser<bool_parser<Policy>> {
  using attribute = bool;

  template <typename Iterator, typename Attribute>
  bool parse(Iterator& f, Iterator const& l, Attribute& a) const {
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

using single_char_bool_parser = bool_parser<detail::single_char_bool_policy>;
using zero_one_bool_parser = bool_parser<detail::zero_one_bool_policy>;
using literal_bool_parser = bool_parser<detail::literal_bool_policy>;

template <>
struct parser_registry<bool> {
  using type = single_char_bool_parser;
};

namespace parsers {

auto const tf = bool_parser<detail::single_char_bool_policy>{};
auto const zero_one = bool_parser<detail::zero_one_bool_policy>{};
auto const boolean = bool_parser<detail::literal_bool_policy>{};

} // namespace parsers
} // namespace vast

#endif
