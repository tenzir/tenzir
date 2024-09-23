//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// A few function in this file are licensed under the following license:
//
//     Copyright (c) Orson Peters, 2015.
//
//     This software is provided 'as-is', without any express or implied
//     warranty. In no event will the authors be held liable for any damages
//     arising from the use of this software.
//
//     Permission is granted to anyone to use this software for any purpose,
//     including commercial applications, and to alter it and redistribute it
//     freely, subject to the following restrictions:
//
//     1. The origin of this software must not be misrepresented; you must not
//        claim that you wrote the original software. If you use this software
//        in a product, an acknowledgment in the product documentation would be
//        appreciated but is not required.
//
//     2. Altered source versions must be plainly marked as such, and must not
//        be misrepresented as being the original software.
//
//     3. This notice may not be removed or altered from any source
//        distribution.

#pragma once

#include "tenzir/concepts.hpp"

#include <cstdint>
#include <limits>
#include <type_traits>

namespace tenzir::detail {

template <int exp, class T>
constexpr T pow(T base);

template <int exp, class T>
constexpr T pow_impl(T base, uint64_t result = 1) {
  return exp ? (exp & 1 ? pow_impl<(exp >> 1)>(base * base, base * result)
                        : pow_impl<(exp >> 1)>(base * base, result))
             : result;
}

// Checks if base can be squared without overflow the type.
template <class T>
constexpr bool can_square(T base) {
  return base <= std::numeric_limits<T>::max() / base;
}

// Get the largest exponent x such that x is a power of two and pow(base, x)
// doesn't overflow the type of base.
template <class T, T base>
constexpr int max_pot_exp(int result = 1) {
  // Despite the fact that this can never overflow we still have to check for
  // overflow or the compiler will complain.
  return can_square(base)                                  ? max_pot_exp < T,
         can_square(base) ? base * base : 1 > (result * 2) : result;
}

template <int base, class T, int i = max_pot_exp<T, base>()>
constexpr int ilog_helper(T n, int x = 0) {
  // binary search
  return i ? ilog_helper<base, T, i / 2>(
               n >= pow<i, T>(base) ? n / pow<i, T>(base) : n,
               n >= pow<i, T>(base) ? x + i : x)
           : x;
}

/// Computes the power function.
/// @tparam exp The exponent to raise `base` to.
/// @param base The value to raise to the power of *exp*.
/// @returns `base` raised to the power of *exp*.
template <int exp, class T>
constexpr T pow(T base) {
  static_assert(exp < 64, "pow exponents >= 64 can only overflow");
  return exp < 0 ? 1 / pow_impl<-exp>(base) : pow_impl<exp>(base);
}

/// Computes the integer logarithm as `x > 0 ? floor(log(x, base)) : -1`.
/// @tparam base The base of the logarithm.
/// @tparam T The argument type.
/// @returns The integer logarithm of *x*.
template <int base, std::integral T>
constexpr int ilog(T x) {
  static_assert(!(base <= 0), "ilog is not useful for base <= 0");
  static_assert(base != 1, "ilog is not useful for base == 1");
  return x > 0 ? ilog_helper<base>(x) : -1;
}

} // namespace tenzir::detail
