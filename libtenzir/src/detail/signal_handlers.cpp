//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/signal_handlers.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/backtrace.hpp"

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

extern "C" void fatal_handler(int sig) {
  ::fprintf(stderr, "tenzir-%s: Error: signal %d (%s)\n",
            tenzir::version::version, sig, ::strsignal(sig));
  tenzir::detail::backtrace();
  // Reinstall the default handler and call that too.
  signal(sig, SIG_DFL);
  kill(getpid(), sig);
}
