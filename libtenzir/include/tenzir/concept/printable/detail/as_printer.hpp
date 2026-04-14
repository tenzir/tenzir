//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/string/literal.hpp"
#include "tenzir/concepts.hpp"

#include <string>
#include <type_traits>

namespace tenzir {
namespace detail {

// -- unary -------------------------------------------------------------------

// Keep in sync with operators in tenzir/concept/printable/string/literal.hpp.

inline auto as_printer(char c) {
  return literal_printer{c};
}

inline auto as_printer(std::string str) {
  return literal_printer{std::move(str)};
}

template <class T>
  requires(std::is_arithmetic_v<T> and not std::same_as<T, bool>)
auto as_printer(T x) -> literal_printer {
  return literal_printer{x};
}

template <printer T>
constexpr auto as_printer(T x) {
  return x; // A good compiler will elide the copy.
}

// -- binary ------------------------------------------------------------------

// clang-format off

template <class T>
constexpr bool is_convertible_to_unary_printer_v =
  std::is_convertible_v<T, std::string>
  or (std::is_arithmetic_v<T> and not std::is_same_v<T, bool>);

template <class T, class U>
using is_convertible_to_binary_printer =
  std::bool_constant<
    ( printer<T> and printer<U>)
    or ( printer<T> and is_convertible_to_unary_printer_v<U>)
    or (is_convertible_to_unary_printer_v<T> and printer<U>)
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
    printer<T> and printer<U>,
    Binaryprinter<T, U>,
    std::conditional_t<
       printer<T> and is_convertible_to_unary_printer_v<U>,
      Binaryprinter<T, decltype(as_printer(std::declval<U>()))>,
      std::conditional_t<
        is_convertible_to_unary_printer_v<T> and printer<U>,
        Binaryprinter<decltype(as_printer(std::declval<T>())), U>,
        std::false_type
      >
    >
  >;

// Require that at least one of the arguments already is a printer (as opposed
// to merely being convertible to a printer). This prevent statements like `1 << 4`
// being parseable as a sequence printer of two literal printers.
template <template <class, class> class Binaryprinter, class T, class U>
constexpr auto as_printer(T&& x, U&& y) ->
  make_binary_printer<
    Binaryprinter,
    decltype(as_printer(std::declval<T&>())),
    decltype(as_printer(std::declval<U&>()))>
requires(printer<T> or  printer<U>)
{
  return {as_printer(std::forward<T>(x)), as_printer(std::forward<U>(y))};
}

// clang-format on

} // namespace detail
} // namespace tenzir
