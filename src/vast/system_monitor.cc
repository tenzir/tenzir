#include "vast/system_monitor.h"

#include <array>
#include <csignal>
#include <cstdlib>
#include "vast/logger.h"
#include "vast/util/console.h"

namespace vast {

namespace {

// Keeps track of all signals 1--31, with index zero acting as boolean flag to
// indicate that a signal has been received.
std::array<int, 31> signals;

// UNIX signals suck: The counting is still prone to races, but it's better
// than nothing.
void signal_handler(int signo)
{
  ++signals[0];
  ++signals[signo];

  // Only catch termination signals once to allow forced termination by the OS.
  if (signo == SIGINT || signo == SIGTERM)
    std::signal(signo, SIG_DFL);
}

} // namespace <anonymous>

using namespace cppa;

system_monitor::system_monitor(actor_ptr receiver)
  : upstream_(receiver)
{ }

void system_monitor::init()
{
  util::console::unbuffer();
  VAST_LOG_VERBOSE("spawning system monitor @" << id());

  signals.fill(0);
  for (auto s : { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2 })
    std::signal(s, &signal_handler);

  become(
      on(atom("init"), arg_match) >> [=](actor_ptr upstream)
      {
        upstream_ = upstream;
      },
      on(atom("act")) >> [=]
      {
        char c;
        if (signals[0] > 0)
        {
          signals[0] = 0;
          for (int i = 0; size_t(i) < signals.size(); ++i)
          {
            if (i == SIGINT || i == SIGTERM)
              quit();
            while (signals[i] > 0)
              send(upstream_, atom("system"), atom("signal"), signals[i]--);
          }
        }

        if (util::console::get(c))
          send(upstream_, atom("system"), atom("key"), c);

        send(self, atom("act"));
      },
      on(atom("kill")) >> [=] { quit(); }
  );
}

void system_monitor::on_exit()
{
  util::console::buffer();
  VAST_LOG_VERBOSE("system monitor @" << id() << " terminated");
}

} // namespace vast
