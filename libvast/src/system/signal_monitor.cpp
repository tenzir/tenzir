#include <cstring>
#include <csignal>
#include <cstdlib>

#include <caf/all.hpp>

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"

using namespace caf;

namespace vast {
namespace system {
namespace {

// Keeps track of all signals 1--31 (0 unused).
bool signals[32];

extern "C" void signal_handler(int sig) {
  // Catch termination signals only once to allow forced termination by the OS
  // upon sending the signal a second time.
  if (sig == SIGINT || sig == SIGTERM)
    std::signal(sig, SIG_DFL);
  signals[sig] = true;
}

} // namespace <anonymous>

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
      for (int i = 0; i < 32; ++i) {
        if (signals[i]) {
          VAST_DEBUG(self, "caught signal", ::strsignal(i));
          signals[i] = false;
          self->anon_send(receiver, signal_atom::value, i);
        }
      }
      self->delayed_send(self, monitoring_interval, run_atom::value);
    }
  };
}

} // namespace system
} // namespace vast
