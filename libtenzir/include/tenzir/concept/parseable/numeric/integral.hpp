//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/detail/coding.hpp"

#include <cctype>
#include <charconv>
#include <cstdint>

namespace tenzir {
namespace detail {

template <class Iterator>
bool parse_sign(Iterator& i) {
  auto minus = *i == '-';
  if (minus || *i == '+') {
    ++i;
    return minus;
  }
  return false;
}

} // namespace detail

template <class T,
          // Note that it is fine to have this specific for base 10, as other
          // bases must be specified as a later template parameters anyways.
          int MaxDigits = std::numeric_limits<T>::digits10 + 1,
          int MinDigits = 1, int Radix = 10>
struct integral_parser
  : parser_base<integral_parser<T, MaxDigits, MinDigits, Radix>> {
  static_assert(MinDigits > 0, "need at least one minimum digit");
  static_assert(MaxDigits > 0, "need at least one maximum digit");
  static_assert(
    Radix >= 2 and Radix <= 36,
    "Radix must be in range"); // this follows from the from_chars spec
  static_assert(MinDigits <= MaxDigits, "maximum cannot exceed minimum");

  using attribute = T;

  constexpr static auto is_digit(const char c) -> bool {
    constexpr auto in_range
      = [](const char c, const char lower, const char upper) {
          return c >= lower and c <= upper;
        };
    return in_range(c, '0', '0' + std::min(Radix, 9))
           or (Radix > 10
               and (in_range(c, 'a', 'a' + Radix - 10)
                    or in_range(c, 'A', 'A' + Radix - 10)));
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l) {
      return false;
    }
    constexpr static bool is_signed = std::is_signed_v<attribute>;
    const auto first_is_sign = *f == '-' or *f == '+';
    if (first_is_sign and not is_signed) {
      return false;
    }
    // An array large enough to hold a value of the given type
    // and a potential sign character.
    // Its a raw array due to better rendering in debuggers.
    char data[MaxDigits + is_signed]{};
    auto end = std::begin(data);
    int take_chars = 0;
    auto save = f;
    while (true) {
      auto c = *f;
      // Take only digits
      if (not is_digit(c)) {
        if constexpr (not is_signed) {
          break;
        }
        if (take_chars > 0 or not first_is_sign) {
          break;
        }
      }
      *end = c;
      ++take_chars;
      ++end;
      ++f;
      // Take at most MaxDigits chars + a potential minus
      if (take_chars >= MaxDigits + int{first_is_sign}) {
        break;
      }
      // Take at most as many chars as the array can hold
      if (end == std::end(data)) {
        break;
      }
      // Take at most as many chars as are in the input
      if (f == l) {
        break;
      }
    }
    auto begin = std::begin(data);
    // From chars doesn't accept leading plus
    if (*begin == '+') {
      ++begin;
    }
    auto out = attribute{};
    const auto [ptr, ec] = std::from_chars(begin, end, out, Radix);
    if (ec == std::errc{}) {
      const auto read_chars = ptr - std::begin(data);
      TENZIR_ASSERT(read_chars == take_chars);
      if (read_chars >= MinDigits + first_is_sign) {
        a = out;
        return true;
      }
    }
    f = save;
    return false;
  }
};

template <concepts::integral T>
struct parser_registry<T> {
  using type = integral_parser<T>;
};

namespace parsers {

template <class T, int MaxDigits = std::numeric_limits<T>::digits10 + 1,
          int MinDigits = 1, int Radix = 10>
auto const integral = integral_parser<T, MaxDigits, MinDigits, Radix>{};

auto const i8 = integral_parser<int8_t>{};
auto const i16 = integral_parser<int16_t>{};
auto const i32 = integral_parser<int32_t>{};
auto const i64 = integral_parser<int64_t>{};
auto const u8 = integral_parser<uint8_t>{};
auto const u16 = integral_parser<uint16_t>{};
auto const u32 = integral_parser<uint32_t>{};
auto const u64 = integral_parser<uint64_t>{};

auto const hex_prefix = ignore(lit{"0x"} | lit{"0X"});
auto const hex8 = integral_parser<uint8_t, 2, 1, 16>{};
auto const hex16 = integral_parser<uint16_t, 4, 1, 16>{};
auto const hex32 = integral_parser<uint32_t, 8, 1, 16>{};
auto const hex64 = integral_parser<uint64_t, 16, 1, 16>{};

} // namespace parsers
} // namespace tenzir
