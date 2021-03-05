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
