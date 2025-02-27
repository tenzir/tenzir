//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/panic.hpp"

#define TENZIR_ASSERT_ALWAYS(expr, ...)                                        \
  if (not static_cast<bool>(expr)) [[unlikely]] {                              \
    ::tenzir::panic("assertion `" #expr "`failed" __VA_OPT__(": ", )           \
                      __VA_ARGS__);                                            \
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

/// Unlike `__builtin_unreachable()`, reaching this macro is not UB, and unlike
/// `die("unreachable")`, it prints a backtrace.
#define TENZIR_UNREACHABLE() ::tenzir::panic("unreachable")

/// Used to mark code as unfinished. Reaching it throws a panic.
#define TENZIR_TODO() ::tenzir::panic("todo")

/// Used to mark code as unimplemented. Reaching it throws a panic.
#define TENZIR_UNIMPLEMENTED() ::tenzir::panic("unimplemented")
