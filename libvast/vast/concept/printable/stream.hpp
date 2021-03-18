// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <ostream>
#include <type_traits>

#include "vast/concept/printable/print.hpp"

namespace vast {

template <class Char, class Traits, class T>
auto operator<<(std::basic_ostream<Char, Traits>& out, const T& x)
  -> std::enable_if_t<
       is_printable_v<std::ostreambuf_iterator<Char>, T>, decltype(out)
     > {
  using vast::print; // enable ADL
  if (!print(std::ostreambuf_iterator<Char>{out}, x))
    out.setstate(std::ios_base::failbit);
  return out;
}

} // namespace vast

