//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

namespace tenzir::detail {

// TODO: Generalize this to accept different input types and
// derive the appropriate result type.
template <std::integral T, std::integral Result = T>
Result saturating_add(T lhs, T rhs) {
  Result result;
  __builtin_add_overflow(lhs, rhs, &result);
  return result;
}

template <std::integral T, std::integral Result = T>
Result saturating_sub(T lhs, T rhs) {
  Result result;
  __builtin_sub_overflow(lhs, rhs, &result);
  return result;
}

template <std::integral T, std::integral Result = T>
Result saturating_mul(T lhs, T rhs) {
  Result result;
  __builtin_mul_overflow(lhs, rhs, &result);
  return result;
}

} // namespace tenzir::detail
