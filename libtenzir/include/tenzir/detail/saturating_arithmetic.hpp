//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"

#include <chrono>
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

/// Saturating addition of two durations with integral representations. Both
/// operands are converted to their common type first; that conversion itself
/// must be lossless for the result to be meaningful.
template <class Rep1, class Period1, class Rep2, class Period2>
  requires std::integral<Rep1> and std::integral<Rep2>
auto saturating_add(std::chrono::duration<Rep1, Period1> lhs,
                    std::chrono::duration<Rep2, Period2> rhs) {
  using Result = std::common_type_t<std::chrono::duration<Rep1, Period1>,
                                    std::chrono::duration<Rep2, Period2>>;
  return Result{saturating_add(Result{lhs}.count(), Result{rhs}.count())};
}

/// Saturating subtraction of two durations with integral representations.
template <class Rep1, class Period1, class Rep2, class Period2>
  requires std::integral<Rep1> and std::integral<Rep2>
auto saturating_sub(std::chrono::duration<Rep1, Period1> lhs,
                    std::chrono::duration<Rep2, Period2> rhs) {
  using Result = std::common_type_t<std::chrono::duration<Rep1, Period1>,
                                    std::chrono::duration<Rep2, Period2>>;
  return Result{saturating_sub(Result{lhs}.count(), Result{rhs}.count())};
}

/// Saturating addition of a duration to a time point. The delta is cast to
/// the time point's duration; that cast itself must be lossless for the
/// result to be meaningful.
template <class Clock, class Duration, class Rep, class Period>
  requires std::integral<typename Duration::rep> and std::integral<Rep>
auto saturating_add(std::chrono::time_point<Clock, Duration> base,
                    std::chrono::duration<Rep, Period> delta)
  -> std::chrono::time_point<Clock, Duration> {
  auto sum = saturating_add(base.time_since_epoch(),
                            std::chrono::duration_cast<Duration>(delta));
  return std::chrono::time_point<Clock, Duration>{sum};
}

/// Saturating subtraction of a duration from a time point.
template <class Clock, class Duration, class Rep, class Period>
  requires std::integral<typename Duration::rep> and std::integral<Rep>
auto saturating_sub(std::chrono::time_point<Clock, Duration> base,
                    std::chrono::duration<Rep, Period> delta)
  -> std::chrono::time_point<Clock, Duration> {
  auto diff = saturating_sub(base.time_since_epoch(),
                             std::chrono::duration_cast<Duration>(delta));
  return std::chrono::time_point<Clock, Duration>{diff};
}

} // namespace tenzir::detail
