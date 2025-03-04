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

template <typename... Ts>
  requires(sizeof...(Ts) > 0)
[[noreturn]] TENZIR_NO_INLINE void
assertion_failure(const char* cond, std::source_location location,
                  fmt::format_string<Ts...> string, Ts&&... args) {
  TENZIR_UNUSED(cond);
  auto message = std::string{"assertion failed: "};
  fmt::format_to(std::back_inserter(message), string,
                 std::forward<Ts>(args)...);
  panic<1>(std::move(message), location);
}

template <typename T>
[[noreturn]] TENZIR_NO_INLINE void
assertion_failure(const char* cond, std::source_location location, T&& x) {
  TENZIR_UNUSED(cond);
  auto message = std::string{"assertion failed: "};
  fmt::format_to(std::back_inserter(message), "{}", std::forward<T>(x));
  panic<1>(std::move(message), location);
}

[[noreturn]] TENZIR_NO_INLINE inline void
assertion_failure(const char* cond, std::source_location location) {
  panic<1>(fmt::format("assertion `{}` failed", cond), location);
}

} // namespace tenzir::detail

#define TENZIR_ASSERT_ALWAYS(expr, ...)                                        \
  if (not static_cast<bool>(expr)) [[unlikely]] {                              \
    ::tenzir::detail::assertion_failure(#expr, std::source_location::current() \
                                                 __VA_OPT__(, ) __VA_ARGS__);  \
  }                                                                            \
  static_assert(true)

#if TENZIR_ENABLE_ASSERTIONS
#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)
#else
#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_UNUSED(__VA_ARGS__)
#endif

#if TENZIR_ENABLE_ASSERTIONS_CHEAP
#  define TENZIR_ASSERT(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)
#else
#  define TENZIR_ASSERT(...) TENZIR_UNUSED(__VA_ARGS__)
#endif

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, it simply
/// throws a panic.
#define TENZIR_UNREACHABLE() ::tenzir::panic("unreachable")

/// Used to mark code as unfinished. Reaching it throws a panic.
#define TENZIR_TODO() ::tenzir::panic("todo")

/// Used to mark code as unimplemented. Reaching it throws a panic.
#define TENZIR_UNIMPLEMENTED() ::tenzir::panic("unimplemented")
