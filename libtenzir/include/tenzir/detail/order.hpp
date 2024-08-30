//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/die.hpp"

#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>
#include <utility>

namespace tenzir::detail {

// The order functions permute bits in arithmetic types to achieve a bitwise
// total ordering by re-coding the bits as offset binary.

template <class T>
auto order(T x) {
  if constexpr (std::is_integral_v<T>) {
    if constexpr (std::is_unsigned_v<T>) {
      // Unsigned integral types already exhibit a bitwise total order.
      return x;
    } else {
      // For signed integral types, We shift the entire domain by 2^w to the
      // left, where w is the size of T in bits. By ditching 2's-complement, we
      // get a total bitwise ordering.
      x += std::make_unsigned_t<T>{1} << std::numeric_limits<T>::digits;
      return static_cast<std::make_unsigned_t<T>>(x);
    }
  } else if constexpr (std::is_floating_point_v<T>) {
    static_assert(std::numeric_limits<T>::is_iec559, "can only order IEEE 754 "
                                                     "double types");
    TENZIR_ASSERT(!std::isnan(x));
    uint64_t result;
    switch (std::fpclassify(x)) {
      default:
        die("missing std::fpclassify() case");
      case FP_ZERO:
        result = 0x7fffffffffffffff;
        break;
      case FP_INFINITE:
        result = x < 0.0 ? 0 : 0xffffffffffffffff;
        break;
      case FP_SUBNORMAL:
        result = x < 0.0 ? 0x7ffffffffffffffe : 0x8000000000000000;
        break;
      case FP_NAN:
        die("NaN cannot be ordered");
      case FP_NORMAL: {
        static constexpr auto exp_mask = (~0ull << 53) >> 1;
        static constexpr auto sig_mask = ~0ull >> 12;
        auto ux = std::bit_cast<uint64_t>(x);
        auto exp = (ux & exp_mask) >> 52;
        auto sig = ux & sig_mask;
        // If the value is positive we add a 1 as MSB left of the exponent and
        // if the value is negative, we make the MSB 0. If the value is
        // negative we also have to reverse the exponent to ensure that, e.g.,
        // -1 is considered *smaller* than -0.1, although the exponent of -1 is
        // *larger* than -0.1. Because the exponent already has a offset-binary
        // encoding, this merely involves subtracting it from 2^11-1.
        // Thereafter, we add the desired bits of the significand. Because the
        // significand is always >= 0, we can use the same subtraction method
        // for negative values as for the offset-binary encoded exponent.
        if (x > 0.0) {
          result = (ux & exp_mask) | (1ull << 63); // Add positive MSB
          result |= sig;                           // Plug in significand as-is.
          ++result;                                // Account for subnormal.
        } else {
          result = ((exp_mask >> 52) - exp) << 52; // Reverse exponent.
          result |= (sig_mask - sig);              // Reverse significand.
          --result;                                // Account for subnormal.
        }
      }
    }
    return result;
  } else {
    static_assert(!std::is_same_v<T, T>, "T is neither an integral nor a "
                                         "floating point number");
  }
}

template <class T>
using ordered_type = decltype(order(T{}));

} // namespace tenzir::detail
