#include "vast/system_monitor.h"

#include <csignal>
#include <cstdlib>
#include "vast/logger.h"
#include "vast/util/console.h"

namespace vast {

namespace {

int signals[] = { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2 };
int last_signal = 0;

void signal_handler(int signo)
{
  // Only catch termination signals once to allow forced termination by the OS.
  if (signo == SIGINT || signo == SIGTERM)
    std::signal(signo, SIG_DFL);

  last_signal = signo;
}

} // namespace <anonymous>

using namespace cppa;

system_monitor::system_monitor(actor_ptr receiver)
{
  LOG(verbose, core) << "spawning system monitor @" << id();

  for (auto s : signals)
    std::signal(s, &signal_handler);

  // Watches for keystrokes and signals.
  watcher_ = spawn_link<detached>([receiver]
      {
        util::console::unbuffer();

        char c;
        while (! (last_signal == SIGINT || last_signal == SIGTERM))
        {
          if (last_signal)
          {
            send(receiver, atom("system"), atom("signal"), last_signal)
            last_signal = 0;
          }

          if (util::console::get(c))
          {
            if (c == 'Q')
              break;
            send(receiver, atom("system"), atom("keystroke"), c);
          }
        }

        util::console::buffer();
        self->quit();
      });

  LOG(verbose, core) 
    << "system monitor @" << id() << " spawned watcher @" << watcher_->id();

  init_state = (
      on(atom("shutdown")) >> [=]
      {
        quit();
        LOG(verbose, core) << "system monitor @" << id() << " terminated";
      });
}

} // namespace vast
