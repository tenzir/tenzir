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

#include <cstdint>

#include "vast/concept/parseable/core/parser.hpp"

namespace vast {
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

template <
  class T,
  int MaxDigits = std::numeric_limits<T>::digits10 + 1,
  int MinDigits = 1,
  int Radix = 10
>
struct integral_parser
  : parser<integral_parser<T, MaxDigits, MinDigits, Radix>> {
  static_assert(Radix == 10, "unsupported radix");
  static_assert(MinDigits > 0, "need at least one minimum digit");
  static_assert(MaxDigits > 0, "need at least one maximum digit");
  static_assert(MinDigits <= MaxDigits, "maximum cannot exceed minimum");

  using attribute = T;

  static bool isdigit(char c) {
    return c >= '0' && c <= '9';
  }

  template <class Iterator, class Attribute, class F>
  static auto accumulate(Iterator& f, const Iterator& l, Attribute& a, F acc) {
    int digits = 0;
    a = 0;
    while (f != l && isdigit(*f)) {
      if (++digits > MaxDigits)
        return false;
      acc(a, *f++ - '0');
    }
    if (digits < MinDigits)
      return false;
    return true;
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
    if constexpr (std::is_signed_v<T>)
      return parse_signed(f, l, a);
    else
      return parse_unsigned(f, l, a);
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if (f == l)
      return false;
    auto save = f;
    if (dispatch(f, l, a))
      return true;
    f = save;
    return false;
  }
};

template <class T>
struct parser_registry<T, std::enable_if_t<std::is_integral_v<T>>> {
  using type = integral_parser<T>;
};

namespace parsers {

template <
  class T,
  int MaxDigits = std::numeric_limits<T>::digits10 + 1,
  int MinDigits = 1,
  int Radix = 10
>
auto const integral = integral_parser<T, MaxDigits, MinDigits, Radix>{};

auto const i8 = integral_parser<int8_t>{};
auto const i16 = integral_parser<int16_t>{};
auto const i32 = integral_parser<int32_t>{};
auto const i64 = integral_parser<int64_t>{};
auto const u8 = integral_parser<uint8_t>{};
auto const u16 = integral_parser<uint16_t>{};
auto const u32 = integral_parser<uint32_t>{};
auto const u64 = integral_parser<uint64_t>{};

} // namespace parsers
} // namespace vast

