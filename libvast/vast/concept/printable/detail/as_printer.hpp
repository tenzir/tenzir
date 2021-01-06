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

#include <string>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/literal.hpp"

namespace vast {
namespace detail {

// -- unary -------------------------------------------------------------------

// Keep in sync with operators in vast/concept/printable/string/literal.hpp.

inline auto as_printer(char c) {
  return literal_printer{c};
}

inline auto as_printer(std::string str) {
  return literal_printer{std::move(str)};
}

template <class T>
constexpr auto as_printer(T x)
  -> std::enable_if_t<std::conjunction_v<std::is_arithmetic<T>,
                                         std::negation<std::is_same<T, bool>>>,
                      literal_printer> {
  return literal_printer{x};
}

template <class T>
constexpr auto as_printer(T x) -> std::enable_if_t<is_printer_v<T>, T> {
  return x; // A good compiler will elide the copy.
}

// -- binary ------------------------------------------------------------------

// clang-format off

template <class T>
constexpr bool is_convertible_to_unary_printer_v =
  std::is_convertible_v<T, std::string>
  || (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>);

template <class T, class U>
using is_convertible_to_binary_printer =
  std::integral_constant<
    bool,
    (is_printer_v<T> && is_printer_v<U>)
    || (is_printer_v<T> && is_convertible_to_unary_printer_v<U>)
    || (is_convertible_to_unary_printer_v<T> && is_printer_v<U>)
  >;

template <class T, class U>
constexpr bool is_convertible_to_binary_printer_v
  = is_convertible_to_binary_printer<T, U>::value;

template <
  template <class, class> class Binaryprinter,
  class T,
  class U
>
using make_binary_printer =
  std::conditional_t<
    is_printer_v<T> && is_printer_v<U>,
    Binaryprinter<T, U>,
    std::conditional_t<
      is_printer_v<T> && is_convertible_to_unary_printer_v<U>,
      Binaryprinter<T, decltype(as_printer(std::declval<U>()))>,
      std::conditional_t<
        is_convertible_to_unary_printer_v<T> && is_printer_v<U>,
        Binaryprinter<decltype(as_printer(std::declval<T>())), U>,
        std::false_type
      >
    >
  >;

template <template <class, class> class Binaryprinter, class T, class U>
std::enable_if_t<
  // Require that at least one of the arguments already is a printer (as opposed
  // to merely being convertible to a printer). This prevent statements like `1 << 4`
  // being parseable as a sequence printer of two literal printers.
  std::disjunction_v<
    is_printer<std::decay_t<T>>,
    is_printer<std::decay_t<U>>>,
  make_binary_printer<
    Binaryprinter,
    decltype(as_printer(std::declval<T&>())),
    decltype(as_printer(std::declval<U&>()))>
>
constexpr as_printer(T&& x, U&& y) {
  return {as_printer(std::forward<T>(x)), as_printer(std::forward<U>(y))};
}

// clang-format on

} // namespace detail
} // namespace vast


