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
#include "vast/detail/type_traits.hpp"

// -- VAST logging macros ------------------------------------------------------

#if defined(VAST_LOG_LEVEL)

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

namespace vast::detail {
template <class T>
auto id_or_name(T&& x) {
  static_assert(
    !std::is_same_v<const char*, std::remove_reference<T>>,
    "const char* is not allowed for the first argument in a logging statement"
    "supply a component or use `VAST_[ERROR|WARNING|INFO|DEBUG]_ANON` instead");

  using Type = std::remove_pointer_t<T>;
  if constexpr (has_ostream_operator<Type>)
    return x;
  else if constexpr (has_to_string<Type>)
    return to_string(*x);
  else
    return caf::detail::pretty_type_name(typeid(Type));
}
} // namespace vast::detail
#define VAST_LOG_COMPONENT(lvl, m, ...)                                        \
  VAST_LOG(lvl, ::vast::detail::id_or_name(m), __VA_ARGS__)

#if VAST_LOG_LEVEL >= CAF_LOG_LEVEL_TRACE

#define VAST_TRACE(...)                                                        \
  VAST_LOG(CAF_LOG_LEVEL_TRACE, "ENTRY", __VA_ARGS__);                         \
  auto CAF_UNIFYN(vast_log_trace_guard_) = ::caf::detail::make_scope_guard(    \
    [=] { VAST_LOG(CAF_LOG_LEVEL_TRACE, "EXIT"); })

#else // VAST_LOG_LEVEL > CAF_LOG_LEVEL_TRACE

#define VAST_TRACE(...) CAF_VOID_STMT

#endif // VAST_LOG_LEVEL > CAF_LOG_LEVEL_TRACE

#if VAST_LOG_LEVEL >= 0
#define VAST_ERROR(...) VAST_LOG_COMPONENT(CAF_LOG_LEVEL_ERROR, __VA_ARGS__)
#define VAST_ERROR_(...) VAST_LOG(CAF_LOG_LEVEL_ERROR, __VA_ARGS__)
#else
#define VAST_ERROR(...) CAF_VOID_STMT
#define VAST_ERROR_(...) CAF_VOID_STMT
#endif


#if VAST_LOG_LEVEL >= 1
#define VAST_WARNING(...) VAST_LOG_COMPONENT(CAF_LOG_LEVEL_WARNING, __VA_ARGS__)
#define VAST_WARNING_(...) VAST_LOG(CAF_LOG_LEVEL_WARNING, __VA_ARGS__)
#else
#define VAST_WARNING(...) CAF_VOID_STMT
#define VAST_WARNING_(...) CAF_VOID_STMT
#endif

#if VAST_LOG_LEVEL >= 2
#define VAST_INFO(...) VAST_LOG_COMPONENT(CAF_LOG_LEVEL_INFO, __VA_ARGS__)
#define VAST_INFO_(...) VAST_LOG(CAF_LOG_LEVEL_INFO, __VA_ARGS__)
#else
#define VAST_INFO(...) CAF_VOID_STMT
#define VAST_INFO_(...) CAF_VOID_STMT
#endif

#if VAST_LOG_LEVEL >= 3
#define VAST_DEBUG(...) VAST_LOG_COMPONENT(CAF_LOG_LEVEL_DEBUG, __VA_ARGS__)
#define VAST_DEBUG_(...) VAST_LOG(CAF_LOG_LEVEL_DEBUG, __VA_ARGS__)
#else
#define VAST_DEBUG(...) CAF_VOID_STMT
#define VAST_DEBUG_(...) CAF_VOID_STMT
#endif

#else // defined(VAST_LOG_LEVEL)

#define VAST_LOG(...) CAF_VOID_STMT

#define VAST_LOG_COMPONENT(...) CAF_VOID_STMT

#define VAST_TRACE(...) CAF_VOID_STMT

#define VAST_ERROR(...) CAF_VOID_STMT
#define VAST_ERROR_(...) CAF_VOID_STMT

#define VAST_WARNING(...) CAF_VOID_STMT
#define VAST_WARNING_(...) CAF_VOID_STMT

#define VAST_INFO(...) CAF_VOID_STMT
#define VAST_INFO_(...) CAF_VOID_STMT

#define VAST_DEBUG(...) CAF_VOID_STMT
#define VAST_DEBUG_(...) CAF_VOID_STMT

#endif // defined(VAST_LOG_LEVEL)

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
