#include <cstring>
#include <csignal>
#include <cstdlib>

#include "vast/actor/signal_monitor.h"

namespace vast {
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

signal_monitor::state::state(local_actor* self)
  : basic_state{self, "signal-monitor"} {
}

signal_monitor::behavior signal_monitor::make(stateful_pointer self,
                                              actor receiver) {
  VAST_DEBUG_AT(self, "sends signals to", receiver);
  for (auto s : {SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2}) {
    VAST_DEBUG_AT(self, "registers signal handler for", ::strsignal(s));
    std::signal(s, &signal_handler);
  }
  self->send(self, run_atom::value);
  return {
    [=](run_atom) {
      for (int i = 0; i < 32; ++i) {
        if (signals[i]) {
          VAST_DEBUG_AT(self, "caught signal", ::strsignal(i));
          signals[i] = false;
          self->send(receiver, signal_atom::value, i);
        }
      }
      self->delayed_send(self, std::chrono::milliseconds(100),
                         self->current_message());
    }
  };
}

} // namespace vast
