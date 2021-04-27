//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/aliases.hpp"
#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/si_literals.hpp"

#include <type_traits>

namespace vast {

template <class T>
struct si_parser : parser<si_parser<T>> {
  static_assert(std::is_integral_v<T>);

  using attribute = T;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    using namespace si_literals;
    auto num = make_parser<T>{};
    auto ws = ignore(*parsers::space);
    // clang-format off
    auto p = (num >> ws >> "Ki") ->* [](T x) { return x * 1_Ki; }
           | (num >> ws >> "Mi") ->* [](T x) { return x * 1_Mi; }
           | (num >> ws >> "Gi") ->* [](T x) { return x * 1_Gi; }
           | (num >> ws >> "Ti") ->* [](T x) { return x * 1_Ti; }
           | (num >> ws >> "Pi") ->* [](T x) { return x * 1_Pi; }
           | (num >> ws >> "Ei") ->* [](T x) { return x * 1_Ei; }
           | (num >> ws >> 'k') ->* [](T x) { return x * 1_k; }
           | (num >> ws >> 'M') ->* [](T x) { return x * 1_M; }
           | (num >> ws >> 'G') ->* [](T x) { return x * 1_G; }
           | (num >> ws >> 'T') ->* [](T x) { return x * 1_T; }
           | (num >> ws >> 'P') ->* [](T x) { return x * 1_P; }
           | (num >> ws >> 'E') ->* [](T x) { return x * 1_E; }
           | num;
    // clang-format on
    return p(f, l, a);
  }
};

namespace parsers {

static auto const count = si_parser<vast::count>{};
static auto const integer = si_parser<vast::integer>{};
static auto const bytesize = count >> ~ch<'B'>;

} // namespace parsers
} // namespace vast
