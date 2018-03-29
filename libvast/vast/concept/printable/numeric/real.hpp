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

#include <cmath>
#include <cstdint>
#include <string>
#include <type_traits>

#include "vast/concept/printable/detail/print_numeric.hpp"
#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <class T, int MaxDigits = 10>
struct real_printer : printer<real_printer<T, MaxDigits>> {
  static_assert(std::is_floating_point<T>{}, "T must be a floating point type");

  using attribute = T;

  template <class Iterator>
  bool print(Iterator& out, T x) const {
    // negative = positive + sign
    if (x < 0) {
      *out++ = '-';
      x = -x;
    }
    T left;
    uint64_t right = std::round(std::modf(x, &left) * std::pow(10, MaxDigits));
    if (MaxDigits == 0)
      return detail::print_numeric(out, static_cast<uint64_t>(std::round(x)));
    if (!detail::print_numeric(out, static_cast<uint64_t>(left)))
      return false;
    *out++ = '.';
    // Add leading decimal zeros.
    auto magnitude = right == 0 ? MaxDigits : std::log10(right);
    for (auto i = 1.0; i < MaxDigits - magnitude; ++i)
      *out++ = '0';
    // Avoid trailing zeros on the decimal digits.
    while (right > 0 && right % 10 == 0)
      right /= 10;
    return detail::print_numeric(out, right);
  }
};

template <class T>
struct printer_registry<T, std::enable_if_t<std::is_floating_point<T>::value>> {
  using type = real_printer<T>;
};

namespace printers {

auto const fp = real_printer<float>{};
auto const real = real_printer<double>{};
auto const real1 = real_printer<double, 1>{};
auto const real2 = real_printer<double, 2>{};
auto const real3 = real_printer<double, 3>{};
auto const real4 = real_printer<double, 4>{};
auto const real5 = real_printer<double, 5>{};
auto const real6 = real_printer<double, 6>{};

} // namespace printers
} // namespace vast

