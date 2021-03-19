//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/signal_handlers.hpp"

#include "vast/config.hpp"
#include "vast/detail/backtrace.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

extern "C" void fatal_handler(int sig) {
  ::fprintf(stderr, "vast-%s: Error: signal %d (%s)\n", vast::version::version,
            sig, ::strsignal(sig));
  vast::detail::backtrace();
  // Reinstall the default handler and call that too.
  signal(sig, SIG_DFL);
  kill(getpid(), sig);
}
