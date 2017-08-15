#ifndef VAST_CONCEPT_PRINTABLE_NUMERIC_INTEGRAL_HPP
#define VAST_CONCEPT_PRINTABLE_NUMERIC_INTEGRAL_HPP

#include <cmath>
#include <cstdint>
#include <type_traits>

#include "vast/concept/printable/detail/print_numeric.hpp"
#include "vast/concept/printable/core/printer.hpp"

namespace vast {
namespace policy {

/// Only display a `-` sign when the number is negative.
struct plain;

/// In addition to displaying a `-` sign for negative numbers, also display a
/// `+` sign for positive numbers.
struct force_sign;

} // namespace policy

template <
  typename T,
  typename Policy = policy::plain,
  int MinDigits = 0
>
struct integral_printer : printer<integral_printer<T, Policy, MinDigits>> {
  static_assert(std::is_integral<T>{}, "T must be an integral type");

  using attribute = T;

  template <typename Iterator, typename U>
  static void pad(Iterator& out, U x) {
    if (MinDigits > 0) {
      int magnitude = x == 0 ? 0 : std::log10(x < 0 ? -x : x);
      for (auto i = 1; i < MinDigits - magnitude; ++i)
        *out++ = '0';
    }
  }

  template <typename Iterator, typename U = T>
  auto print(Iterator& out, U x) const
  -> std::enable_if_t<std::is_unsigned<U>{}, bool> {
    pad(out, x);
    return detail::print_numeric(out, x);
  }

  template <typename Iterator, typename U = T>
  auto print(Iterator& out, U x) const
  -> std::enable_if_t<std::is_signed<U>{}, bool> {
    if (x < 0) {
      *out++ = '-';
      x = -x;
    } else if (std::is_same<Policy, policy::force_sign>::value) {
      *out++ = '+';
    }
    pad(out, x);
    return detail::print_numeric(out, x);
  }
};

template <typename T>
struct printer_registry<T, std::enable_if_t<std::is_integral<T>::value>> {
  using type = integral_printer<T>;
};

namespace printers {

// GCC 7.1 complains about this version
//
//     template <class T, Policy = ...>
//     auto const integral = integral_printer<T, Policy>{};
//
// but for some reason doesn't care if we "rewrite" it as follows. (#132)
template <
  typename T,
  typename Policy = policy::plain,
  int MinDigits = 0
>
const integral_printer<T, Policy, MinDigits> integral =
  integral_printer<T, Policy, MinDigits>{};

auto const i8 = integral_printer<int8_t>{};
auto const i16 = integral_printer<int16_t>{};
auto const i32 = integral_printer<int32_t>{};
auto const i64 = integral_printer<int64_t>{};
auto const u8 = integral_printer<uint8_t>{};
auto const u16 = integral_printer<uint16_t>{};
auto const u32 = integral_printer<uint32_t>{};
auto const u64 = integral_printer<uint64_t>{};

} // namespace printers
} // namespace vast

#endif
