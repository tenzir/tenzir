// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <istream>
#include <type_traits>

#include "vast/concept/parseable/parse.hpp"

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

