//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/uuid.hpp"

#include <algorithm>
#include <cstddef>

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
    for (size_t i = 0; i < uuid::num_bytes; ++i) {
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
      if constexpr (std::is_same_v<Attribute, uuid>)
        x[i] = (x[i] << 4) | lookup(c);
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

  static std::byte lookup(char c) {
    static constexpr auto digits = "0123456789abcdefABCDEF";
    static constexpr uint8_t values[]
      = {0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  10,  11,
         12, 13, 14, 15, 10, 11, 12, 13, 14, 15, 0xff};
    // TODO: use a static table as opposed to searching in the vector.
    return std::byte{values[std::find(digits, digits + 22, c) - digits]};
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
