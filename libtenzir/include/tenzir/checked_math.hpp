//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <concepts>
#include <limits>
#include <optional>

namespace tenzir {

template <std::integral T>
constexpr auto max = std::numeric_limits<T>::max();

template <std::integral T>
constexpr auto min = std::numeric_limits<T>::lowest();

#define CHECK(x)                                                               \
  if (not(x)) {                                                                \
    return std::nullopt;                                                       \
  }

template <std::integral X, std::integral Y>
constexpr auto checked_add(X x, Y y)
  -> std::optional<std::common_type_t<X, Y>> {
  static_assert(sizeof(x) == sizeof(y));
  using R = std::common_type_t<X, Y>;
  if constexpr (std::is_signed_v<X> && std::is_signed_v<Y>) {
    static_assert(std::is_signed_v<R>);
    if (x > 0 && y > 0) {
      // -> x + y <= max<R>
      CHECK(x <= max<R> - y);
    } else if (x < 0 && y < 0) {
      // -> min<R> <= x + y
      CHECK(min<R> - y <= x);
    } else {
      // Cannot overflow due to mixed signs.
    }
    return x + y;
  } else if constexpr (std::is_signed_v<X>) {
    // Make it so that the first argument is the unsigned one.
    return checked_add(y, x);
  } else {
    static_assert(std::is_unsigned_v<X>);
    // -> 0 <= x + y
    if constexpr (std::is_signed_v<Y>) {
      if (y < 0) {
        // -> -y <= x
        auto minus_y = y == min<Y> ? R(max<Y>) + 1 : R(-y);
        CHECK(minus_y <= x);
      }
    }
    // -> x + y <= max<R>
    if (y >= 0) {
      // -> y <= max<R> - x
      CHECK(R(y) <= max<R> - x);
    }
    return x + R(y);
  }
}

template <std::integral X, std::integral Y>
constexpr auto checked_sub(X x, Y y) -> std::optional<X> {
  static_assert(sizeof(x) == sizeof(y));
  // -> min<X> <= x - y && x - y <= max<X>
  if (y >= 0) {
    // -> min<X> <= x - y
    // -> min<X> + y <= x
    // Check whether we can convert `y` to `X`.
    if (y <= Y(max<X>)) {
      CHECK(min<X> + X(y) <= x);
      return x - X(y);
    }
    // We know `y > max<X>` and thus `min<X> + y >= 0`.
    CHECK(x >= 0);
    CHECK(Y(min<X>) + y <= Y(x));
    return X(Y(x) - Y(y));
  }
  // We know `y < 0` (which implies that Y is signed).
  // -> x - y <= max<X>
  if (y == min<Y>) {
    // -> x <= max<X> + min<Y>
    if constexpr (std::is_signed_v<X>) {
      CHECK(x <= -1);
      return x - y;
    } else {
      // -> x <= max<U> + min<S>
      CHECK(x <= max<X> - max<Y> - 1);
      return X(Y(x) - Y(y));
    }
  }
  CHECK(x <= max<X> - X(-y));
  return x + X(-y);
}

template <std::integral X, std::integral Y>
constexpr auto checked_mul(X x, Y y)
  -> std::optional<std::conditional_t<std::is_signed_v<X>, X, Y>> {
  static_assert(sizeof(x) == sizeof(y));
  if (x == 0 || y == 0) {
    return 0;
  }
  if constexpr (std::is_unsigned_v<X> && std::is_signed_v<Y>) {
    // Make signed the first argument if possible.
    return checked_mul(y, x);
  } else if constexpr (std::is_unsigned_v<X>) {
    static_assert(std::is_unsigned_v<Y>);
    // -> x * y <= max<X>;
    CHECK(x <= max<X> / y);
    return x * y;
  } else if constexpr (std::is_unsigned_v<Y>) {
    if (x == -1 && y == Y(max<X>) + 1) {
      return min<X>;
    }
    CHECK(y <= Y(max<X>));
    return checked_mul(x, X(y));
  } else {
    static_assert(std::same_as<X, Y>);
    // -> x * y <= max<X>
    if (y > 0) {
      CHECK(x <= max<X> / y);
    } else {
      CHECK(x >= max<X> / y);
    }
    // -> min<X> <= x * y
    if (y == -1) {
      if (x == -1) {
        return 1;
      }
      std::swap(x, y);
    }
    if (y > 0) {
      CHECK(min<X> / y <= x);
    } else {
      CHECK(min<X> / y >= x);
    }
    return x * y;
  }
}

#undef CHECK

} // namespace tenzir
