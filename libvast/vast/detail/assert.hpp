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

#if VAST_ENABLE_ASSERTIONS
#  include <cstdio>
#  include <cstdlib>
#  define VAST_ASSERT(expr)                                                    \
    do {                                                                       \
      if (static_cast<bool>(expr) == false) {                                  \
        /* NOLINTNEXTLINE */                                                   \
        ::fprintf(stderr, "%s:%u: assertion failed '%s'\n", __FILE__,          \
                  __LINE__, #expr);                                            \
        ::vast::detail::backtrace();                                           \
        ::abort();                                                             \
      }                                                                        \
    } while (false)
#else
#  define VAST_ASSERT(expr) static_cast<void>(expr)
#endif
