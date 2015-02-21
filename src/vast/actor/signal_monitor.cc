#include "vast/actor/signal_monitor.h"

#include <array>
#include <csignal>
#include <cstdlib>
#include <thread>

namespace vast {

namespace {

// Keeps track of all signals 1--31, with index 0 acting as boolean flag to
// indicate that a signal has been received.
std::array<int, 32> signals;

// UNIX signals suck: The counting is still prone to races, but it's better
// than nothing.
void signal_handler(int signo)
{
  ++signals[0];
  ++signals[signo];
  // Catch termination signals only once to allow forced termination by the OS.
  if (signo == SIGINT || signo == SIGTERM)
    std::signal(signo, SIG_DFL);
}

} // namespace <anonymous>

using namespace caf;

signal_monitor::signal_monitor(actor receiver)
  : default_actor{"signal-monitor"},
    sink_{std::move(receiver)}
{
}

void signal_monitor::on_exit()
{
  sink_ = invalid_actor;
}

behavior signal_monitor::make_behavior()
{
  VAST_DEBUG(this, "sends signals to", sink_);
  signals.fill(0);
  for (auto s : { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2 })
    std::signal(s, &signal_handler);
  return
  {
    [=](run_atom)
    {
      if (signals[0] > 0)
      {
        signals[0] = 0;
        for (int i = 0; size_t(i) < signals.size(); ++i)
          while (signals[i]-- > 0)
            send(sink_, signal_atom::value, i);
      }
      delayed_send(this, std::chrono::milliseconds(100), current_message());
    },
    catch_unexpected()
  };
}

} // namespace vast
