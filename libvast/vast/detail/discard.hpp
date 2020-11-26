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

#include "vast/detail/pp.hpp"

#include <type_traits>

// Ensures all args are used and syntactically valid, without evaluating them.

#  define VAST_DISCARD_1(msg) static_cast<std::void_t<decltype((msg))>>(0)
#  define VAST_DISCARD_2(m1, m2)                                           \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2))
#  define VAST_DISCARD_3(m1, m2, m3)                                       \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)), VAST_DISCARD_1((m3))
#  define VAST_DISCARD_4(m1, m2, m3, m4)                                   \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4))
#  define VAST_DISCARD_5(m1, m2, m3, m4, m5)                               \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5))
#  define VAST_DISCARD_6(m1, m2, m3, m4, m5, m6)                           \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6))
#  define VAST_DISCARD_7(m1, m2, m3, m4, m5, m6, m7)                       \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6)),                      \
      VAST_DISCARD_1((m7))
#  define VAST_DISCARD_8(m1, m2, m3, m4, m5, m6, m7, m8)                   \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6)),                      \
      VAST_DISCARD_1((m7)), VAST_DISCARD_1((m8))
#  define VAST_DISCARD_9(m1, m2, m3, m4, m5, m6, m7, m8, m9)               \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6)),                      \
      VAST_DISCARD_1((m7)), VAST_DISCARD_1((m8)),                      \
      VAST_DISCARD_1((m9))
#  define VAST_DISCARD_10(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10)         \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6)),                      \
      VAST_DISCARD_1((m7)), VAST_DISCARD_1((m8)),                      \
      VAST_DISCARD_1((m9)), VAST_DISCARD_1((m10))
#  define VAST_DISCARD_11(m1, m2, m3, m4, m5, m6, m7, m8, m9, m10, m11)    \
    VAST_DISCARD_1((m1)), VAST_DISCARD_1((m2)),                        \
      VAST_DISCARD_1((m3)), VAST_DISCARD_1((m4)),                      \
      VAST_DISCARD_1((m5)), VAST_DISCARD_1((m6)),                      \
      VAST_DISCARD_1((m7)), VAST_DISCARD_1((m8)),                      \
      VAST_DISCARD_1((m9)), VAST_DISCARD_1((m10)),                     \
      VAST_DISCARD_1((m11))

#  define VAST_DISCARD_ARGS(...)                                           \
    VAST_PP_OVERLOAD(VAST_DISCARD_, __VA_ARGS__)(__VA_ARGS__)
