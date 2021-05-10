//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/parse.hpp"

#include <istream>
#include <type_traits>

namespace vast {

template <class CharT, class Traits, class T>
auto operator>>(std::basic_istream<CharT, Traits>& in, T& x)
  -> std::enable_if_t<is_parseable_v<std::istreambuf_iterator<CharT>, T>,
                      decltype(in)> {
  using vast::parse; // enable ADL
  std::istreambuf_iterator<CharT> begin{in}, end;
  if (!parse(begin, end, x))
    in.setstate(std::ios_base::failbit);
  return in;
}

} // namespace vast
