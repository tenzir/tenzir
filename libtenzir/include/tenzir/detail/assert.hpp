//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"

#include <fmt/format.h>

#include <source_location>
#include <string>

namespace tenzir::detail {

[[noreturn]] void panic(std::string message);

template <class... Ts>
  requires(sizeof...(Ts) > 0)
[[noreturn]] TENZIR_NO_INLINE void
panic(fmt::format_string<Ts...> str, Ts&&... xs) {
  panic(fmt::format(str, std::forward<Ts>(xs)...));
}

[[noreturn]] void
fail_assertion_impl(const char* expr, std::string_view explanation,
                    std::source_location source);

template <class... Ts>
  requires(sizeof...(Ts) > 1)
[[noreturn]] TENZIR_NO_INLINE void
fail_assertion(const char* expr, fmt::format_string<Ts...> str, Ts&&... args,
               std::source_location source = std::source_location::current()) {
  fail_assertion_impl(expr, fmt::format(str, std::forward<Ts>(args)...),
                      source);
}

template <class T>
[[noreturn]] TENZIR_NO_INLINE void
fail_assertion(const char* expr, T&& x,
               std::source_location source = std::source_location::current()) {
  fail_assertion_impl(expr, fmt::to_string(std::forward<T>(x)), source);
}

[[noreturn]] TENZIR_NO_INLINE inline void
fail_assertion(const char* expr, std::source_location source
                                 = std::source_location::current()) {
  fail_assertion_impl(expr, "", source);
}

} // namespace tenzir::detail

#define TENZIR_ASSERT_ALWAYS(expr, ...)                                        \
  do {                                                                         \
    if (not static_cast<bool>(expr)) [[unlikely]] {                            \
      ::tenzir::detail::fail_assertion(#expr __VA_OPT__(, ) __VA_ARGS__);      \
    }                                                                          \
  } while (false)

// Provide TENZIR_ASSERT_EXPENSIVE macro
#if TENZIR_ENABLE_ASSERTIONS

#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)

#else // !TENZIR_ENABLE_ASSERTIONS

#  define TENZIR_ASSERT_EXPENSIVE(...) TENZIR_UNUSED(__VA_ARGS__)

#endif // TENZIR_ENABLE_ASSERTIONS

// Provide TENZIR_ASSERT_CHEAP macro
#if TENZIR_ENABLE_ASSERTIONS_CHEAP

#  define TENZIR_ASSERT_CHEAP(...) TENZIR_ASSERT_ALWAYS(__VA_ARGS__)

#else // !TENZIR_ENABLE_ASSERTIONS_CHEAP

#  define TENZIR_ASSERT_CHEAP(...) TENZIR_UNUSED(__VA_ARGS__)

#endif // TENZIR_ENABLE_ASSERTIONS_CHEAP

// Provide the `TENZIR_ASSERT()` macro. We treat assertions as
// cheap by default, expensive assertions need to be marked as such
// by using `TENZIR_ASSERT_EXPENSIVE()` instead.
#define TENZIR_ASSERT(...) TENZIR_ASSERT_CHEAP(__VA_ARGS__)

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, and unlike
/// `die("unreachable")`, it prints a backtrace.
#define TENZIR_UNREACHABLE() ::tenzir::detail::panic("unreachable")

/// Used to mark code as unfinished. Reaching it aborts the program.
#define TENZIR_TODO() ::tenzir::detail::panic("todo")
