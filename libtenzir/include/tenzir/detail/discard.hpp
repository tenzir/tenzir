//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/pp.hpp"

#include <type_traits>

// Ensures all args are used and syntactically valid, without evaluating them.

#if __GNUC__
#  define TENZIR_DISCARD_1(msg) static_cast<void>((msg))
#else
#  define TENZIR_DISCARD_1(msg) static_cast<std::void_t<decltype((msg))>>(0)
#endif

#define TENZIR_DISCARD_2(m1, m2) TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2))
#define TENZIR_DISCARD_3(m1, m2, m3)                                           \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3))
#define TENZIR_DISCARD_4(m1, m2, m3, m4)                                       \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4))
#define TENZIR_DISCARD_5(m1, m2, m3, m4, m5)                                   \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5))
#define TENZIR_DISCARD_6(m1, m2, m3, m4, m5, m6)                               \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6))
#define TENZIR_DISCARD_7(m1, m2, m3, m4, m5, m6, m7)                           \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6)),    \
    TENZIR_DISCARD_1((m7))
#define TENZIR_DISCARD_8(m1, m2, m3, m4, m5, m6, m7, m8)                       \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6)),    \
    TENZIR_DISCARD_1((m7)), TENZIR_DISCARD_1((m8))
#define TENZIR_DISCARD_9(m1, m2, m3, m4, m5, m6, m7, m8, m9)                   \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6)),    \
    TENZIR_DISCARD_1((m7)), TENZIR_DISCARD_1((m8)), TENZIR_DISCARD_1((m9))
#define TENZIR_DISCARD_10(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10)             \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6)),    \
    TENZIR_DISCARD_1((m7)), TENZIR_DISCARD_1((m8)), TENZIR_DISCARD_1((m9)),    \
    TENZIR_DISCARD_1((m10))
#define TENZIR_DISCARD_11(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11)        \
  TENZIR_DISCARD_1((m1)), TENZIR_DISCARD_1((m2)), TENZIR_DISCARD_1((m3)),      \
    TENZIR_DISCARD_1((m4)), TENZIR_DISCARD_1((m5)), TENZIR_DISCARD_1((m6)),    \
    TENZIR_DISCARD_1((m7)), TENZIR_DISCARD_1((m8)), TENZIR_DISCARD_1((m9)),    \
    TENZIR_DISCARD_1((m10)), TENZIR_DISCARD_1((m11))

#define TENZIR_DISCARD_ARGS(...)                                               \
  TENZIR_PP_OVERLOAD(TENZIR_DISCARD_, __VA_ARGS__)(__VA_ARGS__)
