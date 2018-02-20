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

#ifndef VAST_CONCEPT_PARSEABLE_VAST_BASE_HPP
#define VAST_CONCEPT_PARSEABLE_VAST_BASE_HPP

#include "vast/base.hpp"
#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string.hpp"

namespace vast {

struct base_parser : parser<base_parser> {
  using attribute = base;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto num = parsers::integral<size_t>;
    auto to_base = [](std::vector<size_t> xs) { return base{xs}; };
    auto to_uniform_base = [](std::tuple<size_t, optional<size_t>> tuple) {
      auto b = std::get<0>(tuple);
      auto n = std::get<1>(tuple);
      return base::uniform(b, n ? *n : 0);
    };
    auto ws = ignore(*parsers::space);
    auto delim = ws >> ',' >> ws;
    auto uniform = "uniform("_p >> num >> -(delim >> num) >> ')';
    auto regular = '['_p >> (num % delim) >> ']';
    auto p = uniform ->* to_uniform_base
           | regular ->* to_base;
    return p(f, l, a);
  }
};

template <>
struct parser_registry<base> {
  using type = base_parser;
};

namespace parsers {

static auto const base = make_parser<vast::base>();

} // namespace parsers
} // namespace vast

#endif

