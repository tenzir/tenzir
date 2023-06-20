//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/config.hpp"
#include "vast/detail/backtrace.hpp"
#include "vast/detail/pp.hpp"

#include <cstdio>
#include <cstdlib>

#define VAST_ASSERT_2(expr, msg)                                               \
  do {                                                                         \
    if (static_cast<bool>(expr) == false) [[unlikely]] {                       \
      /* NOLINTNEXTLINE */                                                     \
      ::fprintf(stderr, "%s:%u: assertion failed '%s'\n", __FILE__, __LINE__,  \
                msg);                                                          \
      ::vast::detail::backtrace();                                             \
      ::abort();                                                               \
    }                                                                          \
  } while (false)

#define VAST_ASSERT_1(expr) VAST_ASSERT_2(expr, #expr)

#define VAST_NOOP_1(expr)                                                      \
  static_cast<void>(sizeof(decltype(expr))) // NOLINT(bugprone-*)

#define VAST_NOOP_2(expr, msg)                                                 \
  do {                                                                         \
    VAST_NOOP_1(expr);                                                         \
    VAST_NOOP_1(msg);                                                          \
  } while (false)

// Provide VAST_ASSERT_EXPENSIVE macro
#if VAST_ENABLE_ASSERTIONS

#  define VAST_ASSERT_EXPENSIVE(...)                                           \
    VAST_PP_OVERLOAD(VAST_ASSERT_, __VA_ARGS__)(__VA_ARGS__)

#else // !VAST_ENABLE_ASSERTIONS

#  define VAST_ASSERT_EXPENSIVE(...)                                           \
    VAST_PP_OVERLOAD(VAST_NOOP_, __VA_ARGS__)(__VA_ARGS__)

#endif // VAST_ENABLE_ASSERTIONS

// Provide VAST_ASSERT_CHEAP macro
#if VAST_ENABLE_ASSERTIONS_CHEAP

#  define VAST_ASSERT_CHEAP(...)                                               \
    VAST_PP_OVERLOAD(VAST_ASSERT_, __VA_ARGS__)(__VA_ARGS__)

#else // !VAST_ENABLE_ASSERTIONS_CHEAP

#  define VAST_ASSERT_CHEAP(...)                                               \
    VAST_PP_OVERLOAD(VAST_NOOP_, __VA_ARGS__)(__VA_ARGS__)

#endif // VAST_ENABLE_ASSERTIONS_CHEAP

// Provide the `VAST_ASSERT()` macro. We treat assertions as
// expensive by default, cheap assertions need to be marked as such
// by using `VAST_ASSERT_CHEAP()` instead.
#define VAST_ASSERT(...) VAST_ASSERT_EXPENSIVE(__VA_ARGS__)

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, and unlike
/// `die("unreachable")`, it prints a backtrace.
#define VAST_UNREACHABLE() VAST_ASSERT_CHEAP(false)
