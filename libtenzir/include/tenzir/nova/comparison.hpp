//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cmath>
#include <compare>
#include <concepts>
#include <limits>

namespace tenzir::nova {

namespace _ {

constexpr auto is_ordering(ast::binary_op op) -> bool {
  using enum ast::binary_op;
  switch (op) {
    case gt:
    case geq:
    case lt:
    case leq:
      return true;
    case add:
    case sub:
    case mul:
    case div:
    case eq:
    case neq:
    case and_:
    case or_:
    case in:
    case if_:
    case else_:
      return false;
  }
  TENZIR_UNREACHABLE();
}

template <ast::binary_op Op>
auto compare_values(auto const& lhs, auto const& rhs) -> Bool {
  using enum ast::binary_op;
  if constexpr (Op == eq) {
    return lhs == rhs;
  } else if constexpr (Op == neq) {
    return lhs != rhs;
  } else if constexpr (Op == gt) {
    return lhs > rhs;
  } else if constexpr (Op == geq) {
    return lhs >= rhs;
  } else if constexpr (Op == lt) {
    return lhs < rhs;
  } else {
    static_assert(Op == leq, "not a comparison operator");
    return lhs <= rhs;
  }
}

template <std::integral I>
auto compare_integer_float(I lhs, Float rhs) -> std::partial_ordering {
  if (std::isnan(rhs)) {
    return std::partial_ordering::unordered;
  }
  constexpr auto digits = std::numeric_limits<I>::digits;
  auto const upper = std::ldexp(1.0, digits);
  auto const lower = std::signed_integral<I> ? -upper : 0.0;
  if (rhs < lower) {
    return std::partial_ordering::greater;
  }
  if (rhs >= upper) {
    return std::partial_ordering::less;
  }
  auto const integer_rhs = static_cast<I>(rhs);
  if (std::cmp_less(lhs, integer_rhs)) {
    return std::partial_ordering::less;
  }
  if (std::cmp_greater(lhs, integer_rhs)) {
    return std::partial_ordering::greater;
  }
  if (rhs > static_cast<Float>(integer_rhs)) {
    return std::partial_ordering::less;
  }
  if (rhs < static_cast<Float>(integer_rhs)) {
    return std::partial_ordering::greater;
  }
  return std::partial_ordering::equivalent;
}

template <concepts::one_of<Int, UInt, Float> A,
          concepts::one_of<Int, UInt, Float> B>
auto compare_numbers(A lhs, B rhs) -> std::partial_ordering {
  if constexpr (std::same_as<A, Float> and std::same_as<B, Float>) {
    return lhs <=> rhs;
  } else if constexpr (std::integral<A> and std::integral<B>) {
    if (std::cmp_less(lhs, rhs)) {
      return std::partial_ordering::less;
    }
    if (std::cmp_greater(lhs, rhs)) {
      return std::partial_ordering::greater;
    }
    return std::partial_ordering::equivalent;
  } else if constexpr (std::integral<A>) {
    return compare_integer_float(lhs, rhs);
  } else {
    auto const result = compare_integer_float(rhs, lhs);
    return result == std::partial_ordering::less
             ? std::partial_ordering::greater
           : result == std::partial_ordering::greater
             ? std::partial_ordering::less
             : result;
  }
}

template <ast::binary_op Op, concepts::one_of<Int, UInt, Float> A,
          concepts::one_of<Int, UInt, Float> B>
auto compare_numbers(A lhs, B rhs) -> Bool {
  auto const result = compare_numbers(lhs, rhs);
  using enum ast::binary_op;
  if constexpr (Op == eq) {
    return result == std::partial_ordering::equivalent;
  } else if constexpr (Op == neq) {
    return result != std::partial_ordering::equivalent;
  } else if constexpr (Op == gt) {
    return result == std::partial_ordering::greater;
  } else if constexpr (Op == geq) {
    return result == std::partial_ordering::greater
           or result == std::partial_ordering::equivalent;
  } else if constexpr (Op == lt) {
    return result == std::partial_ordering::less;
  } else {
    static_assert(Op == leq, "not a comparison operator");
    return result == std::partial_ordering::less
           or result == std::partial_ordering::equivalent;
  }
}

} // namespace _

template <ast::binary_op Op, class T>
  requires(not concepts::number<T>)
auto compare(T const& lhs, T const& rhs) -> Bool {
  return _::compare_values<Op>(lhs, rhs);
}

template <ast::binary_op Op, concepts::one_of<Int, UInt, Float> A,
          concepts::one_of<Int, UInt, Float> B>
auto compare(A lhs, B rhs) -> Bool {
  return _::compare_numbers<Op>(lhs, rhs);
}

} // namespace tenzir::nova
