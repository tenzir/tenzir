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

#if VAST_ENABLE_ASSERTIONS
#  include <cstdio>
#  include <cstdlib>

#  define VAST_ASSERT_2(expr, msg)                                             \
    do {                                                                       \
      if (static_cast<bool>(expr) == false) {                                  \
        /* NOLINTNEXTLINE */                                                   \
        ::fprintf(stderr, "%s:%u: assertion failed '%s'\n", __FILE__,          \
                  __LINE__, msg);                                              \
        ::vast::detail::backtrace();                                           \
        ::abort();                                                             \
      }                                                                        \
    } while (false)

#  define VAST_ASSERT_1(expr) VAST_ASSERT_2(expr, #  expr)

#else

#  define VAST_ASSERT_1(expr)                                                  \
    static_cast<void>(sizeof(decltype(expr))) // NOLINT(bugprone-*)

#  define VAST_ASSERT_2(expr, msg)                                             \
    do {                                                                       \
      VAST_ASSERT_1(expr);                                                     \
      VAST_ASSERT_1(msg);                                                      \
    } while (false)

#endif

#define VAST_ASSERT(...)                                                       \
  VAST_PP_OVERLOAD(VAST_ASSERT_, __VA_ARGS__)(__VA_ARGS__)
