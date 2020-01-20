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

#include "vast/base.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string.hpp"

namespace vast {

struct base_parser : parser<base_parser> {
  using attribute = base;

  template <size_t Bits>
  static base to_uniform_base(size_t b) {
    return base::uniform<Bits>(b);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    auto num = parsers::integral<size_t>;
    auto to_base = [](std::vector<size_t> xs) { return base{xs}; };
    auto to_explicit_uniform_base = [](std::tuple<size_t, size_t> xs) {
      return base::uniform(std::get<0>(xs), std::get<1>(xs));
    };
    auto ws = ignore(*parsers::space);
    auto delim = ws >> ',' >> ws;
    auto uniform8 = "uniform8("_p >> num >> ')';
    auto uniform16 = "uniform16("_p >> num >> ')';
    auto uniform32 = "uniform32("_p >> num >> ')';
    auto uniform64 = "uniform64("_p >> num >> ')';
    auto uniform = "uniform("_p >> num >> delim >> num >> ')';
    auto direct = '['_p >> (num % delim) >> ']';
    auto p = uniform ->* to_explicit_uniform_base
           | uniform8 ->* to_uniform_base<8>
           | uniform16 ->* to_uniform_base<16>
           | uniform32 ->* to_uniform_base<32>
           | uniform64 ->* to_uniform_base<64>
           | direct ->* to_base;
    return p(f, l, a);
  }
};

template <>
struct parser_registry<base> {
  using type = base_parser;
};

namespace parsers {

static auto const base = base_parser{};

} // namespace parsers
} // namespace vast
