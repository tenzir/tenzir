// SPDX-FileCopyrightText: (c) 2016 Tenzir GmbH <info@tenzir.com>
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
