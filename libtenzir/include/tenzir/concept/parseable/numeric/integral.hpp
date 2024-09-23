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
  static_assert(Radix == 10 || Radix == 16, "unsupported radix");
  static_assert(MinDigits > 0, "need at least one minimum digit");
  static_assert(MaxDigits > 0, "need at least one maximum digit");
  static_assert(MinDigits <= MaxDigits, "maximum cannot exceed minimum");

  using attribute = T;

  static bool isdigit(char c) {
    if constexpr (Radix == 10) {
      return c >= '0' && c <= '9';
    } else if constexpr (Radix == 16) {
      return std::isxdigit(static_cast<unsigned char>(c)) != 0;
    } else {
      static_assert(detail::always_false_v<decltype(Radix)>, "unsupported "
                                                             "radix");
    }
  }

  template <class Iterator, class Attribute, class F>
  static auto accumulate(Iterator& f, const Iterator& l, Attribute& a, F acc) {
    if (f == l) {
      return false;
    }
    int digits = 0;
    for (a = 0; isdigit(*f) && f != l && digits < MaxDigits; ++f, ++digits) {
      if constexpr (Radix == 10) {
        acc(a, *f - '0');
      } else if constexpr (Radix == 16) {
        acc(a, detail::hex_to_byte(*f));
      } else {
        static_assert(detail::always_false_v<decltype(Radix)>, "unsupported "
                                                               "radix");
      }
    }
    return digits >= MinDigits;
  }

  template <class Iterator>
  static auto parse_pos(Iterator& f, const Iterator& l, unused_type) {
    return accumulate(f, l, unused, [](auto&, auto) {});
  }

  template <class Iterator, class Attribute>
  static auto parse_pos(Iterator& f, const Iterator& l, Attribute& a) {
    return accumulate(f, l, a, [](auto& n, auto x) {
      n *= Radix;
      n += x;
    });
  }

  template <class Iterator>
  static auto parse_neg(Iterator& f, const Iterator& l, unused_type) {
    return accumulate(f, l, unused, [](auto&, auto) {});
  }

  template <class Iterator, class Attribute>
  static auto parse_neg(Iterator& f, const Iterator& l, Attribute& a) {
    return accumulate(f, l, a, [](auto& n, auto x) {
      n *= Radix;
      n -= x;
    });
  }

  template <class Iterator, class Attribute>
  static bool parse_signed(Iterator& f, const Iterator& l, Attribute& a) {
    return detail::parse_sign(f) ? parse_neg(f, l, a) : parse_pos(f, l, a);
  }

  template <class Iterator, class Attribute>
  static bool parse_unsigned(Iterator& f, const Iterator& l, Attribute& a) {
    return parse_pos(f, l, a);
  }

  template <class Iterator, class Attribute>
  static bool dispatch(Iterator& f, const Iterator& l, Attribute& a) {
    if constexpr (std::is_signed_v<T>) {
      return parse_signed(f, l, a);
    } else {
      return parse_unsigned(f, l, a);
    }
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l) {
      return false;
    }
    auto save = f;
    if (dispatch(f, l, a)) {
      return true;
    }
    f = save;
    return false;
  }
};

template <std::integral T>
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
