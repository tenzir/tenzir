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

#include <type_traits>

#include "vast/aliases.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/si_literals.hpp"

namespace vast {

template <class T>
struct si_parser : parser<si_parser<T>> {
  static_assert(std::is_integral_v<T>);

  using attribute = T;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace parser_literals;
    using namespace si_literals;
    auto num = make_parser<T>{};
    // clang-format off
    auto p = (num >> "Ki") ->* [](T x) { return x * 1_Ki; }
           | (num >> "Mi") ->* [](T x) { return x * 1_Mi; }
           | (num >> "Gi") ->* [](T x) { return x * 1_Gi; }
           | (num >> "Ti") ->* [](T x) { return x * 1_Ti; }
           | (num >> "Pi") ->* [](T x) { return x * 1_Pi; }
           | (num >> "Ei") ->* [](T x) { return x * 1_Ei; }
           | (num >> 'k') ->* [](T x) { return x * 1_k; }
           | (num >> 'M') ->* [](T x) { return x * 1_M; }
           | (num >> 'G') ->* [](T x) { return x * 1_G; }
           | (num >> 'T') ->* [](T x) { return x * 1_T; }
           | (num >> 'P') ->* [](T x) { return x * 1_P; }
           | (num >> 'E') ->* [](T x) { return x * 1_E; }
           | num;
    // clang-format on
    return p(f, l, a);
  }
};

namespace parsers {

static auto const count = si_parser<vast::count>{};
static auto const integer = si_parser<vast::integer>{};

} // namespace parsers
} // namespace vast
