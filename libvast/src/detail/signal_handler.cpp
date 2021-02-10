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

#include "vast/detail/signal_handler.hpp"

#include "vast/config.hpp"
#include "vast/detail/backtrace.hpp"

#include <cstdio>
#include <cstdlib>
#include <cstring>

extern "C" void signal_handler(int sig) {
  ::fprintf(stderr, "vast-" VAST_VERSION ": Error: signal %d (%s)\n", sig,
            ::strsignal(sig));
  vast::detail::backtrace();
  std::quick_exit(1);
}
