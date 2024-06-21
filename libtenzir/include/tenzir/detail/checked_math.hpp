//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the Tenzir
// code base. Details about the original file:
//
// - Repository: https://github.com/mbeutel/slowmath
// - Commit:     747161cc7c756e9ef55f3d12031ff642b8923d1b
// - Path:       include/slowmath/detail/arithmetic.hpp
// - Author:     Moritz Beutel
// - Copyright:  (c) Moritz Beutel
// - License:    Boost Software License - Version 1.0

#pragma once

#include <limits>
#include <type_traits>
#include <variant>

namespace tenzir::checked_math {

template <std::integral L, std::integral R>
using result_type = std::variant<std::common_type_t<L, R>, const char*>;

template <std::integral T>
constexpr T max_v = std::numeric_limits<T>::max();
template <std::integral T>
constexpr T min_v = std::numeric_limits<T>::lowest();

#define SLOWMATH_DETAIL_OVERFLOW_CHECK(boolean_expression)                     \
  if (not(boolean_expression)) {                                               \
    return "integer overflow";                                                 \
  }

template <std::integral A, std::integral B>
constexpr auto add(A a, B b) -> result_type<A, B> {
  using V = std::common_type_t<A, B>;
  using S = std::make_signed_t<V>;
  using U = std::make_unsigned_t<V>;

  V result = V(U(a) + U(b));
  if constexpr (std::is_signed<V>::value) {
    // cast to signed to avoid warning about pointless unsigned comparison
    SLOWMATH_DETAIL_OVERFLOW_CHECK(!(S(a) < 0 && S(b) < 0 && S(result) >= 0)
                                   && !(S(a) > 0 && S(b) > 0 && S(result) < 0));
  } else {
    SLOWMATH_DETAIL_OVERFLOW_CHECK(result >= a && result >= b);
  }
  return result;
}

template <std::integral A, std::integral B>
constexpr auto subtract(A a, B b) -> result_type<A, B> {
  using V = std::common_type_t<A, B>;
  using S = std::make_signed_t<V>;

  if constexpr (std::is_signed<V>::value) {
    // cast to signed to avoid warning about pointless unsigned comparison
    SLOWMATH_DETAIL_OVERFLOW_CHECK(!(S(b) > 0 && a < min_v<V> + b)
                                   && !(S(b) < 0 && a > max_v<V> + b));
  } else {
    SLOWMATH_DETAIL_OVERFLOW_CHECK(a >= b);
  }
  return V{a - b};
}

template <std::integral A, std::integral B>
constexpr auto multiply(A a, B b) -> result_type<A, B> {
  using V = std::common_type_t<A, B>;
  using S = std::make_signed_t<V>;

  if constexpr (std::is_signed<V>::value) {
    // cast to signed to avoid warning about pointless unsigned comparison
    SLOWMATH_DETAIL_OVERFLOW_CHECK(
      !(S(a) > 0
        && ((S(b) > 0 && S(a) > S(max_v<V> / b))
            || (S(b) <= 0 && S(b < min_v<V> / a))))
      && !(S(a) < 0
           && ((S(b) > 0 && S(a) < S(min_v<V> / b))
               || (S(b) <= 0 && S(b < max_v<V> / a)))));
  } else {
    SLOWMATH_DETAIL_OVERFLOW_CHECK(!(b > 0 && static_cast<V>(a) > max_v<V> / b));
  }
  return V(a * b);
}

template <std::integral N, std::integral D>
constexpr auto divide(N n, D d) -> result_type<N, D> {
  using V = std::common_type_t<N, D>;

  if constexpr (std::is_signed<V>::value) {
    SLOWMATH_DETAIL_OVERFLOW_CHECK(!(n == min_v<V> && d == -1));
  }
  return V(n / d);
}

template <std::integral N, std::integral D>
constexpr auto modulo(N n, D d) -> result_type<N, D> {
  using V = std::common_type_t<N, D>;

  if constexpr (std::is_signed<V>::value) {
    SLOWMATH_DETAIL_OVERFLOW_CHECK(!(n == min_v<V> && d == -1));
  }
  return V(n % d);
}

#undef SLOWMATH_DETAIL_OVERFLOW_CHECK
} // namespace tenzir::checked_math