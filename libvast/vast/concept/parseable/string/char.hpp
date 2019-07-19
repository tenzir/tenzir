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

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/detail/char_helpers.hpp"

namespace vast {

/// Parses a single character.
/// @see static_char_parser
class dynamic_char_parser : public parser<dynamic_char_parser> {
public:
  using attribute = char;

  template <class Iterator, class Attribute>
  static bool parse(Iterator& f, const Iterator& l, Attribute& x, char c) {
    if (f == l || *f != c)
      return false;
    detail::absorb(x, c);
    ++f;
    return true;
  }

  constexpr dynamic_char_parser(char c) : c_{c} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return parse(f, l, x, c_);
  }

private:
  char c_;
};

/// Parses a single character.
/// @see dynamic_char_parser
template <char Char>
class static_char_parser : public parser<static_char_parser<Char>> {
public:
  using attribute = char;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    return dynamic_char_parser::parse(f, l, x, Char);
  }
};

namespace parsers {

template <char Char>
static const auto ch = static_char_parser<Char>{};

using chr = dynamic_char_parser;

} // namespace parsers
} // namespace vast
