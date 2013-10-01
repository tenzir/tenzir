#include "vast/system_monitor.h"

#include <array>
#include <csignal>
#include <cstdlib>
#include "vast/util/console.h"

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

system_monitor::system_monitor(actor_ptr key_receiver,
                               actor_ptr signal_receiver)
  : key_receiver_{std::move(key_receiver)},
    signal_receiver_{std::move(signal_receiver)}
{
}

void system_monitor::on_exit()
{
  util::console::buffer();
  actor<system_monitor>::on_exit();
}

void system_monitor::act()
{
  VAST_LOG_ACTOR_DEBUG("sends keystrokes to @" << key_receiver_->id());
  VAST_LOG_ACTOR_DEBUG("sends signals to @" << signal_receiver_->id());
  util::console::unbuffer();

  signals.fill(0);
  for (auto s : { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2 })
    std::signal(s, &signal_handler);

  become(
      on(atom("act")) >> [=]
      {
        char c;
        if (signals[0] > 0)
        {
          signals[0] = 0;
          for (int i = 0; size_t(i) < signals.size(); ++i)
            while (signals[i]-- > 0)
              send(signal_receiver_, atom("system"), atom("signal"), i);
        }

        if (util::console::get(c, 100))
          send(key_receiver_, atom("system"), atom("key"), c);

        self << last_dequeued();
      },
      on(atom("kill")) >> [=]
      {
        quit();
      });
}

char const* system_monitor::description() const
{
  return "system-monitor";
}

} // namespace vast
