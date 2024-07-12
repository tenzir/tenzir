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

[[noreturn]] void
panic_impl(std::string message, std::source_location source
                                = std::source_location::current());

template <class... Ts>
[[noreturn]] TENZIR_NO_INLINE void
panic(fmt::format_string<Ts...> str, Ts&&... xs,
      std::source_location source = std::source_location::current()) {
  panic_impl(fmt::format(str, std::forward<Ts>(xs)...), source);
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

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, and unlike
/// `die("unreachable")`, it prints a backtrace.
#define TENZIR_UNREACHABLE() ::tenzir::detail::panic("unreachable")

/// Used to mark code as unfinished. Reaching it throws a panic.
#define TENZIR_TODO() ::tenzir::detail::panic("todo")

/// Used to mark code as unimplemented. Reaching it throws a panic.
#define TENZIR_UNIMPLEMENTED() ::tenzir::detail::panic("unimplemented")
