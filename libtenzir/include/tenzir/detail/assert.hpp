//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/config.hpp"
#include "tenzir/detail/backtrace.hpp"
#include "tenzir/detail/pp.hpp"

#include <cstdio>
#include <cstdlib>

#define TENZIR_ASSERT_2(expr, msg)                                             \
  do {                                                                         \
    if (static_cast<bool>(expr) == false) [[unlikely]] {                       \
      /* NOLINTNEXTLINE */                                                     \
      ::fprintf(stderr, "%s:%u: assertion failed '%s'\n", __FILE__, __LINE__,  \
                msg);                                                          \
      ::tenzir::detail::backtrace();                                           \
      ::abort();                                                               \
    }                                                                          \
  } while (false)

#define TENZIR_ASSERT_1(expr) TENZIR_ASSERT_2(expr, #expr)

#define TENZIR_NOOP_1(expr)                                                    \
  static_cast<void>(sizeof(decltype(expr))) // NOLINT(bugprone-*)

#define TENZIR_NOOP_2(expr, msg)                                               \
  do {                                                                         \
    TENZIR_NOOP_1(expr);                                                       \
    TENZIR_NOOP_1(msg);                                                        \
  } while (false)

// Provide TENZIR_ASSERT_EXPENSIVE macro
#if TENZIR_ENABLE_ASSERTIONS

#  define TENZIR_ASSERT_EXPENSIVE(...)                                         \
    TENZIR_PP_OVERLOAD(TENZIR_ASSERT_, __VA_ARGS__)(__VA_ARGS__)

#else // !TENZIR_ENABLE_ASSERTIONS

#  define TENZIR_ASSERT_EXPENSIVE(...)                                         \
    TENZIR_PP_OVERLOAD(TENZIR_NOOP_, __VA_ARGS__)(__VA_ARGS__)

#endif // TENZIR_ENABLE_ASSERTIONS

// Provide TENZIR_ASSERT_CHEAP macro
#if TENZIR_ENABLE_ASSERTIONS_CHEAP

#  define TENZIR_ASSERT_CHEAP(...)                                             \
    TENZIR_PP_OVERLOAD(TENZIR_ASSERT_, __VA_ARGS__)(__VA_ARGS__)

#else // !TENZIR_ENABLE_ASSERTIONS_CHEAP

#  define TENZIR_ASSERT_CHEAP(...)                                             \
    TENZIR_PP_OVERLOAD(TENZIR_NOOP_, __VA_ARGS__)(__VA_ARGS__)

#endif // TENZIR_ENABLE_ASSERTIONS_CHEAP

// Provide the `TENZIR_ASSERT()` macro. We treat assertions as
// expensive by default, cheap assertions need to be marked as such
// by using `TENZIR_ASSERT_CHEAP()` instead.
#define TENZIR_ASSERT(...) TENZIR_ASSERT_EXPENSIVE(__VA_ARGS__)

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, and unlike
/// `die("unreachable")`, it prints a backtrace.
#define TENZIR_UNREACHABLE() TENZIR_ASSERT_CHEAP(false, "unreachable")

/// TODO
#define TENZIR_TODO() TENZIR_ASSERT_CHEAP(false, "todo")
