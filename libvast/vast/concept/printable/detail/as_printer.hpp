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

#ifndef VAST_CONCEPT_PRINTABLE_DETAIL_AS_PRINTER_HPP
#define VAST_CONCEPT_PRINTABLE_DETAIL_AS_PRINTER_HPP

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

template <size_t N>
auto as_printer(char const(&str)[N]) {
  return literal_printer{str};
}

inline auto as_printer(std::string str) {
  return literal_printer{std::move(str)};
}

template <typename T>
auto as_printer(T x)
-> std::enable_if_t<
     std::is_arithmetic<T>{} && ! std::is_same<T, bool>::value,
     literal_printer
   > {
  return literal_printer{x};
}

template <typename T>
auto as_printer(T x) -> std::enable_if_t<is_printer<T>{}, T> {
  return x; // A good compiler will elide the copy.
}

// -- binary ------------------------------------------------------------------

template <typename T>
using is_convertible_to_unary_printer =
  std::integral_constant<
    bool,
    std::is_convertible<T, std::string>{}
    || (std::is_arithmetic<T>{} && ! std::is_same<T, bool>::value)
  >;

template <typename T, typename U>
using is_convertible_to_binary_printer =
  std::integral_constant<
    bool,
    (is_printer<T>{} && is_printer<U>{})
    || (is_printer<T>{} && is_convertible_to_unary_printer<U>{})
    || (is_convertible_to_unary_printer<T>{} && is_printer<U>{})
  >;

template <
  template <typename, typename> class Binaryprinter,
  typename T,
  typename U
>
using make_binary_printer =
  std::conditional_t<
    is_printer<T>{} && is_printer<U>{},
    Binaryprinter<T, U>,
    std::conditional_t<
      is_printer<T>{} && is_convertible_to_unary_printer<U>{},
      Binaryprinter<T, decltype(as_printer(std::declval<U>()))>,
      std::conditional_t<
        is_convertible_to_unary_printer<T>{} && is_printer<U>{},
        Binaryprinter<decltype(as_printer(std::declval<T>())), U>,
        std::false_type
      >
    >
  >;

template <
  template <typename, typename> class Binaryprinter,
  typename T,
  typename U
>
auto as_printer(T&& x, U&& y)
  -> std::enable_if_t<
       is_convertible_to_binary_printer<std::decay_t<T>, std::decay_t<U>>{},
       make_binary_printer<
         Binaryprinter,
         decltype(as_printer(std::forward<T>(x))),
         decltype(as_printer(std::forward<U>(y)))
       >
     > {
  return {as_printer(std::forward<T>(x)), as_printer(std::forward<U>(y))};
}

} // namespace detail
} // namespace vast

#endif

