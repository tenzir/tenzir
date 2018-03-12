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

#include <cstring>
#include <csignal>
#include <cstdlib>

#include <caf/all.hpp>

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"

using namespace caf;

namespace {

// Keeps track of all signals by their value from 1 to 31. The flag at index 0 
// is used to tell whether a signal has been raised or not.
bool signals[32];

extern "C" void signal_handler(int sig) {
  // Catch termination signals only once to allow forced termination by the OS
  // upon sending the signal a second time.
  if (sig == SIGINT || sig == SIGTERM)
    std::signal(sig, SIG_DFL);
  signals[0] = true;
  signals[sig] = true;
}

} // namespace <anonymous>

namespace vast::system {

signal_monitor_type::behavior_type
signal_monitor(signal_monitor_type::stateful_pointer<signal_monitor_state> self,
               std::chrono::milliseconds monitoring_interval,
               actor receiver) {
  VAST_DEBUG(self, "sends signals to", receiver);
  for (auto s : {SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2}) {
    VAST_DEBUG(self, "registers signal handler for", ::strsignal(s));
    std::signal(s, &signal_handler);
  }
  self->send(self, run_atom::value);
  return {
    [=](run_atom) {
      if (signals[0]) {
        signals[0] = false;
        for (int i = 1; i < 32; ++i) {
          if (signals[i]) {
            VAST_DEBUG(self, "caught signal", ::strsignal(i));
            signals[i] = false;
            self->anon_send(receiver, signal_atom::value, i);
          }
        }
      }
      self->delayed_send(self, monitoring_interval, run_atom::value);
    }
  };
}

} // namespace vast::system
