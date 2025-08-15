//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/panic.hpp"

namespace tenzir::detail {

template <bool include_cond = false, typename... Ts>
  requires(sizeof...(Ts) > 0)
[[noreturn]] TENZIR_NO_INLINE void
assertion_failure(std::string_view cond, std::source_location location,
                  fmt::format_string<Ts...> string, Ts&&... args) {
  auto message = std::string{"assertion failed: "};
  if constexpr (include_cond) {
    message += cond;
    message += ": ";
  }
  fmt::format_to(std::back_inserter(message), string,
                 std::forward<Ts>(args)...);
  panic<1>(std::move(message), location);
}

template <bool include_cond = false, typename T>
[[noreturn]] TENZIR_NO_INLINE void
assertion_failure(std::string_view cond, std::source_location location, T&& x) {
  auto message = std::string{"assertion failed: "};
  if constexpr (include_cond) {
    message += cond;
    message += ": ";
  }
  fmt::format_to(std::back_inserter(message), "{}", std::forward<T>(x));
  panic<1>(std::move(message), location);
}

template <bool = false>
[[noreturn]] TENZIR_NO_INLINE inline void
assertion_failure(std::string_view cond, std::source_location location) {
  panic<1>(fmt::format("assertion `{}` failed", cond), location);
}

} // namespace tenzir::detail

#define TENZIR_ASSERT_ALWAYS(expr, ...)                                        \
  if (not static_cast<bool>(expr)) [[unlikely]] {                              \
    ::tenzir::detail::assertion_failure(#expr, std::source_location::current() \
                                                 __VA_OPT__(, ) __VA_ARGS__);  \
  }                                                                            \
  static_assert(true)

#define TENZIR_ASSERT_EQ_ALWAYS(LHS, RHS, ...)                                 \
  {                                                                            \
    if (not static_cast<bool>(LHS == RHS)) [[unlikely]] {                      \
      ::tenzir::detail::assertion_failure<true>(                               \
        fmt::format("{} (" #LHS ") == {} (" #RHS ")", LHS, RHS),               \
        std::source_location::current() __VA_OPT__(, ) __VA_ARGS__);           \
    }                                                                          \
  }                                                                            \
  static_assert(true)

#if TENZIR_ENABLE_ASSERTIONS
#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)
#  define TENZIR_ASSERT_EQ_EXPENSIVE(LHS, RHS, ...)                            \
    TENZIR_ASSERT_EQ_ALWAYS(LHS, RHS, __VA_ARGS__)
#else
#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_UNUSED(__VA_ARGS__)
#  define TENZIR_ASSERT_EQ_EXPENSIVE(LHS, RHS, ...)                            \
    TENZIR_UNUSED(LHS, RHS, __VA_ARGS__)
#endif

#if TENZIR_ENABLE_ASSERTIONS_CHEAP
#  define TENZIR_ASSERT(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)
#  define TENZIR_ASSERT_EQ(LHS, RHS, ...)                                      \
    TENZIR_ASSERT_EQ_ALWAYS(LHS, RHS, __VA_ARGS__)
#else
#  define TENZIR_ASSERT(...) TENZIR_UNUSED(__VA_ARGS__)
#  define TENZIR_ASSERT_EQ(LHS, RHS, ...) TENZIR_UNUSED(LHS, RHS, __VA_ARGS__)
#endif

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, it simply
/// throws a panic.
#define TENZIR_UNREACHABLE() ::tenzir::panic("unreachable");

/// Used to mark code as unfinished. Reaching it throws a panic.
#define TENZIR_TODO() ::tenzir::panic("todo")

/// Used to mark code as unimplemented. Reaching it throws a panic.
#define TENZIR_UNIMPLEMENTED() ::tenzir::panic("unimplemented")
