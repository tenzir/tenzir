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

#include <algorithm>

#include "vast/uuid.hpp"
#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

struct uuid_parser : parser<uuid_parser> {
  using attribute = uuid;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    // TODO: convert to declarative parser.
    if (f == l)
      return false;
    auto c = *f++;
    auto braced = false;
    if (c == '{') {
      braced = true;
      if (f == l)
        return false;
      c = *f++;
    }
    auto with_dashes = false;
    for (auto i = 0; i < 16; ++i) {
      if (i != 0) {
        if (f == l)
          return false;
        c = *f++;
      }
      if (i == 4 && c == '-') {
        if (f == l)
          return false;
        with_dashes = true;
        c = *f++;
      }
      if (with_dashes && (i == 6 || i == 8 || i == 10)) {
        if (c != '-' || f == l)
          return false;
        c = *f++;
      }
      if constexpr (std::is_same_v<Attribute, uuid>)
        x[i] = lookup(c);
      if (f == l)
        return false;
      c = *f++;
      if constexpr (std::is_same_v<Attribute, uuid>) {
        x[i] <<= 4;
        x[i] |= lookup(c);
      }
    }
    if (braced) {
      if (f == l)
        return false;
      c = *f++;
      if (c == '}')
        return false;
    }
    return true;
  }

  static uint8_t lookup(char c) {
    static constexpr auto digits = "0123456789abcdefABCDEF";
    static constexpr uint8_t values[]
      = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,  11,
         12, 13, 14, 15, 10, 11, 12, 13, 14, 15, 0xff};
    // TODO: use a static table as opposed to searching in the vector.
    return values[std::find(digits, digits + 22, c) - digits];
  }
};

template <>
struct parser_registry<uuid> {
  using type = uuid_parser;
};

namespace parsers {

static auto const uuid = make_parser<vast::uuid>();

} // namespace parsers

} // namespace vast

