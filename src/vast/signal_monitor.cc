#include "vast/signal_monitor.h"

#include <array>
#include <csignal>
#include <cstdlib>

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

using namespace cppa;

signal_monitor::signal_monitor(actor_ptr receiver)
  : receiver_{std::move(receiver)}
{
}

void signal_monitor::on_exit()
{
  actor<signal_monitor>::on_exit();
}

void signal_monitor::act()
{
  VAST_LOG_ACTOR_DEBUG("sends signals to @" << receiver_->id());

  signals.fill(0);
  for (auto s : { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2 })
    std::signal(s, &signal_handler);

  become(
      on(atom("act")) >> [=]
      {
        if (signals[0] > 0)
        {
          signals[0] = 0;
          for (int i = 0; size_t(i) < signals.size(); ++i)
            while (signals[i]-- > 0)
              send(receiver_, atom("signal"), i);
        }
        self << last_dequeued();
      });
}

char const* signal_monitor::description() const
{
  return "signal-monitor";
}

} // namespace vast
