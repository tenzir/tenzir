/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include <caf/logger.hpp>

#include "vast/detail/pp.hpp"

// -- VAST logging macros ------------------------------------------------------

#if defined(CAF_LOG_LEVEL)

#define VAST_LOG_IMPL(lvl, msg) CAF_LOG_IMPL("vast", lvl, msg)

#define VAST_LOG_2(lvl, m1) VAST_LOG_IMPL(lvl, m1)

#define VAST_LOG_3(lvl, m1, m2) VAST_LOG_IMPL(lvl, m1 << m2)

#define VAST_LOG_4(lvl, m1, m2, m3) VAST_LOG_IMPL(lvl, m1 << m2 << m3)

#define VAST_LOG_5(lvl, m1, m2, m3, m4) VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4)

#define VAST_LOG_6(lvl, m1, m2, m3, m4, m5)                                    \
  VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4 << m5)

#define VAST_LOG_7(lvl, m1, m2, m3, m4, m5, m6)                                \
  VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4 << m5 << m6)

#define VAST_LOG_8(lvl, m1, m2, m3, m4, m5, m6, m7)                            \
  VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4 << m5 << m6 << m7)

#define VAST_LOG_9(lvl, m1, m2, m3, m4, m5, m6, m7, m8)                        \
  VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4 << m5 << m6 << m7 << m8)

#define VAST_LOG_10(lvl, m1, m2, m3, m4, m5, m6, m7, m8, m9)                   \
  VAST_LOG_IMPL(lvl, m1 << m2 << m3 << m4 << m5 << m6 << m7 << m8 << m9)

#define VAST_LOG(...) VAST_PP_OVERLOAD(VAST_LOG_, __VA_ARGS__)(__VA_ARGS__)

#if CAF_LOG_LEVEL >= CAF_LOG_LEVEL_TRACE

#define VAST_TRACE(...)                                                        \
  VAST_LOG(CAF_LOG_LEVEL_TRACE, "ENTRY", __VA_ARGS__);                         \
  auto CAF_UNIFYN(vast_log_trace_guard_) = ::caf::detail::make_scope_guard(    \
    [=] { VAST_LOG(CAF_LOG_LEVEL_TRACE, "EXIT"); })

#else // CAF_LOG_LEVEL > CAF_LOG_LEVEL_TRACE

#define VAST_TRACE(...) CAF_VOID_STMT

#endif // CAF_LOG_LEVEL > CAF_LOG_LEVEL_TRACE

#else // defined(CAF_LOG_LEVEL)

#define VAST_LOG(...) CAF_VOID_STMT

#define VAST_TRACE(...) CAF_VOID_STMT

#endif // defined(CAF_LOG_LEVEL)

#define VAST_ERROR(...) VAST_LOG(CAF_LOG_LEVEL_ERROR, __VA_ARGS__)

#define VAST_WARNING(...) VAST_LOG(CAF_LOG_LEVEL_WARNING, __VA_ARGS__)

#define VAST_INFO(...) VAST_LOG(CAF_LOG_LEVEL_INFO, __VA_ARGS__)

#define VAST_DEBUG(...) VAST_LOG(CAF_LOG_LEVEL_DEBUG, __VA_ARGS__)

// -- VAST_ARG utility for formatting log output -------------------------------

#define VAST_ARG_1(x) CAF_ARG(x)

#define VAST_ARG_2(x_name, x) CAF_ARG2(x_name, x)

#define VAST_ARG_3(x_name, first, last) CAF_ARG3(x_name, first, last)

/// Nicely formats a variable or argument. For example, `VAST_ARG(foo)`
/// generates `foo = ...` in log output, where `...` is the content of the
/// variable. `VAST_ARG("size", xs.size())` generates the output
/// `size = xs.size()`.
#define VAST_ARG(...) VAST_PP_OVERLOAD(VAST_ARG_, __VA_ARGS__)(__VA_ARGS__)

// -- VAST_UNUSED utility for suppressing compiler warnings --------------------

#define VAST_UNUSED_1(x1) CAF_IGNORE_UNUSED(x1)

#define VAST_UNUSED_2(x1, x2)                                                  \
  VAST_UNUSED_1(x1);                                                           \
  CAF_IGNORE_UNUSED(x2)

#define VAST_UNUSED_3(x1, x2, x3)                                              \
  VAST_UNUSED_2(x1, x2);                                                       \
  CAF_IGNORE_UNUSED(x3)

#define VAST_UNUSED_4(x1, x2, x3, x4)                                          \
  VAST_UNUSED_3(x1, x2, x3);                                                   \
  CAF_IGNORE_UNUSED(x4)

#define VAST_UNUSED_5(x1, x2, x3, x4, x5)                                      \
  VAST_UNUSED_4(x1, x2, x3, x4);                                               \
  CAF_IGNORE_UNUSED(x5)

#define VAST_UNUSED_6(x1, x2, x3, x4, x5, x6)                                  \
  VAST_UNUSED_5(x1, x2, x3, x4, x5);                                           \
  CAF_IGNORE_UNUSED(x6)

#define VAST_UNUSED_7(x1, x2, x3, x4, x5, x6, x7)                              \
  VAST_UNUSED_6(x1, x2, x3, x4, x5, x6);                                       \
  CAF_IGNORE_UNUSED(x7)

#define VAST_UNUSED_8(x1, x2, x3, x4, x5, x6, x7, x8)                          \
  VAST_UNUSED_7(x1, x2, x3, x4, x5, x6, x7);                                   \
  CAF_IGNORE_UNUSED(x8)

#define VAST_UNUSED_9(x1, x2, x3, x4, x5, x6, x7, x8, x9)                      \
  VAST_UNUSED_8(x1, x2, x3, x4, x5, x6, x7, x8);                               \
  CAF_IGNORE_UNUSED(x9)

/// Suppresses compiler warnings for unused arguments.
#define VAST_UNUSED(...)                                                       \
  VAST_PP_OVERLOAD(VAST_UNUSED_, __VA_ARGS__)(__VA_ARGS__)
