//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <vast/concepts.hpp>

#include <type_traits>

namespace vast::detail {

template <class T, class U = T>
struct equality_comparable {
  friend bool operator!=(const T& x, const U& y) {
    return !(x == y);
  }
};

template <class T, class U = T>
struct less_than_comparable {
  friend bool operator>(const T& x, const U& y) {
    return y < x;
  }

  friend bool operator<=(const T& x, const U& y) {
    return !(y < x);
  }

  friend bool operator>=(const T& x, const U& y) {
    return !(x < y);
  }
};

template <class T, class U = T>
struct partially_ordered {
  friend bool operator>(const T& x, const U& y) {
    return y < x;
  }

  friend bool operator<=(const T& x, const U& y) {
    return x < y || x == y;
  }

  friend bool operator>=(const T& x, const U& y) {
    return y < x || x == y;
  }
};

template <class T, class U = T>
struct totally_ordered : equality_comparable<T, U>,
                         less_than_comparable<T, U> {};

#define VAST_BINARY_OPERATOR_NON_COMMUTATIVE(NAME, OP)                         \
  template <class T, class U = T>                                              \
  struct NAME {                                                                \
    friend T operator OP(const T& x, const U& y) {                             \
      T t(x);                                                                  \
      t OP## = y;                                                              \
      return t;                                                                \
    }                                                                          \
  };

#define VAST_BINARY_OPERATOR_COMMUTATIVE(NAME, OP)                             \
  template <class T, class U = T>                                              \
  struct NAME {                                                                \
    friend T operator OP(const T& x, const U& y) {                             \
      T copy(x);                                                               \
      copy OP## = y;                                                           \
      return copy;                                                             \
    }                                                                          \
                                                                               \
    template <std::same_as<T> Lhs, std::same_as<U> Rhs>                        \
    friend Lhs operator OP(const Rhs& y, const Lhs& x) requires(               \
      !std::same_as<Lhs, Rhs> && std::is_constructible_v<Lhs, Rhs>) {          \
      Lhs result(y);                                                           \
      result OP## = x;                                                         \
      return result;                                                           \
    }                                                                          \
  };

VAST_BINARY_OPERATOR_COMMUTATIVE(addable, +)
VAST_BINARY_OPERATOR_COMMUTATIVE(multipliable, *)
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(subtractable, -)
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(dividable, / )
VAST_BINARY_OPERATOR_NON_COMMUTATIVE(modable, % )
VAST_BINARY_OPERATOR_COMMUTATIVE(xorable, ^)
VAST_BINARY_OPERATOR_COMMUTATIVE(andable, &)
VAST_BINARY_OPERATOR_COMMUTATIVE(orable, | )

#undef VAST_BINARY_OPERATOR_COMMUTATIVE

template <class T, class U = T>
struct additive : addable<T, U>, subtractable<T, U> {};

template <class T, class U = T>
struct multiplicative : multipliable<T, U>, dividable<T, U> {};

template <class T, class U = T>
struct integer_multiplicative : multiplicative<T, U>, modable<T, U> {};

template <class T, class U = T>
struct arithmetic : additive<T, U>, multiplicative<T, U> {};

template <class T, class U = T>
struct integer_arithmetic : additive<T, U>, integer_multiplicative<T, U> {};

template <class T, class U = T>
struct bitwise : andable<T, U>, orable<T, U>, xorable<T, U> {};

} // namespace vast::detail

