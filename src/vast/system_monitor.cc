#include "vast/system_monitor.h"

#include <csignal>
#include <cstdlib>
#include "vast/logger.h"
#include "vast/util/console.h"

namespace vast {

static bool signaled = false;

static void signal_handler(int signo)
{
  signaled = true;
  std::signal(signo, SIG_DFL);
}

using namespace cppa;

system_monitor::system_monitor(actor_ptr receiver)
{
  LOG(verbose, core) << "spawning system monitor @" << id();

  std::signal(SIGINT, &signal_handler);
  std::signal(SIGTERM, &signal_handler);

  watcher_ = spawn<detached>([receiver]
      {
        util::console::unbuffer();

        char c;
        while (! signaled)
        {
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

  monitor(watcher_);
  LOG(verbose, core) 
    << "system monitor @" << id() << " spawns watcher @" << watcher_->id();

  init_state = (
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        DBG(core) << "watcher @" << last_sender()->id() << " terminated";

        watcher_ = nullptr;
        send(self, atom("shutdown"));
      },
      on(atom("shutdown")) >> [=]
      {
        if (signaled || ! watcher_)
        {
          quit();
          LOG(verbose, core) << "system monitor @" << id() << " terminated";
        }
        else
        {
          // Shut down the watcher if it has not yet been terminated.
          std::raise(SIGTERM);
        }
      });
}

} // namespace vast
