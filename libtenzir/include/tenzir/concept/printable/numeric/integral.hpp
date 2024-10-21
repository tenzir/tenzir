//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/detail/print_numeric.hpp"
#include "tenzir/concepts.hpp"

#include <cmath>
#include <cstdint>
#include <type_traits>

namespace tenzir {
namespace policy {

/// Only display a `-` sign when the number is negative.
struct plain;

/// In addition to displaying a `-` sign for negative numbers, also display a
/// `+` sign for positive numbers.
struct force_sign;

} // namespace policy

template <std::integral T, class Policy = policy::plain, int MinDigits = 0>
struct integral_printer : printer_base<integral_printer<T, Policy, MinDigits>> {
  using attribute = T;

  template <class Iterator, class U>
  static void pad(Iterator& out, U x) {
    if (MinDigits > 0) {
      auto magnitude = [x]() -> int {
        if (x == 0) {
          return 0;
        } else {
          const auto value = x < 0 ? -x : x;
          return std::log10(value);
        }
      }();
      for (auto i = 1; i < MinDigits - magnitude; ++i) {
        *out++ = '0';
      }
    }
  }

  template <class Iterator, class U>
  bool print(Iterator& out, U x) const {
    if constexpr (std::is_signed_v<U>) {
      if (x < 0) {
        *out++ = '-';
        x = -x;
      } else if (std::is_same_v<Policy, policy::force_sign>) {
        *out++ = '+';
      }
    }
    pad(out, x);
    detail::print_numeric(out, x);
    return true;
  }
};

template <std::integral T>
struct printer_registry<T> {
  using type = integral_printer<T>;
};

namespace printers {

// GCC 7.1 complains about this version
//
//     template <class T, Policy = ...>
//     auto const integral = integral_printer<T, Policy>{};
//
// but for some reason doesn't care if we "rewrite" it as follows. (#132)
template <class T, class Policy = policy::plain, int MinDigits = 0>
const integral_printer<T, Policy, MinDigits> integral
  = integral_printer<T, Policy, MinDigits>{};

auto const i8 = integral_printer<int8_t>{};
auto const i16 = integral_printer<int16_t>{};
auto const i32 = integral_printer<int32_t>{};
auto const i64 = integral_printer<int64_t>{};
auto const u8 = integral_printer<uint8_t>{};
auto const u16 = integral_printer<uint16_t>{};
auto const u32 = integral_printer<uint32_t>{};
auto const u64 = integral_printer<uint64_t>{};

} // namespace printers
} // namespace tenzir
