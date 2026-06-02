//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <limits>
#include <type_traits>

namespace tenzir::detail {

// TODO: Generalize this to accept different input types and
// derive the appropriate result type.
template <std::integral T, std::integral Result = T>
Result saturating_add(T lhs, T rhs) {
  Result result;
  if (not __builtin_add_overflow(lhs, rhs, &result)) {
    return result;
  }
  if constexpr (std::is_signed_v<T>) {
    return rhs < 0 ? std::numeric_limits<Result>::min()
                   : std::numeric_limits<Result>::max();
  }
  return std::numeric_limits<Result>::max();
}

template <std::integral T, std::integral Result = T>
Result saturating_sub(T lhs, T rhs) {
  Result result;
  if (not __builtin_sub_overflow(lhs, rhs, &result)) {
    return result;
  }
  if constexpr (std::is_signed_v<T>) {
    return rhs < 0 ? std::numeric_limits<Result>::max()
                   : std::numeric_limits<Result>::min();
  }
  return std::numeric_limits<Result>::min();
}

template <std::integral T, std::integral Result = T>
Result saturating_mul(T lhs, T rhs) {
  Result result;
  if (not __builtin_mul_overflow(lhs, rhs, &result)) {
    return result;
  }
  if constexpr (std::is_signed_v<T>) {
    return (lhs < 0) == (rhs < 0) ? std::numeric_limits<Result>::max()
                                  : std::numeric_limits<Result>::min();
  }
  return std::numeric_limits<Result>::max();
}

} // namespace tenzir::detail
