#include "vast/system_monitor.h"

#include <csignal>
#include <cstdlib>
#include "vast/logger.h"

namespace vast {

namespace detail {

static std::condition_variable cv;
static std::atomic<bool> terminating(false);

static void wait_for_signal()
{
  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  cv.wait(lock, [] { return terminating.load() == true; });
}

static void terminate()
{
  auto old = terminating.load();
  while (! terminating.compare_exchange_weak(old, !old))
    ;

  cv.notify_all();
}

static void signal_handler(int signo)
{
  terminate();
  std::signal(signo, SIG_DFL);
}

static void install_signal_handlers()
{
  std::signal(SIGINT, &signal_handler);
  std::signal(SIGTERM, &signal_handler);
}

} // namespace detail

using namespace cppa;

system_monitor::system_monitor(actor_ptr receiver)
{
  LOG(verbose, core) << "spawning system monitor @" << id();
  detail::install_signal_handlers();
  auto signal_watcher = spawn<detached>([]
      {
        detail::wait_for_signal();
        self->quit();
      });

  monitor(signal_watcher);
  LOG(verbose, core) 
    << "system monitor @" << id() 
    << " spawns signal watcher @" << signal_watcher->id();

  init_state = (
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        DBG(core) 
          << "system monitor @" << id() 
          << " passes termination signal to @" << receiver->id();

        send(receiver, atom("system"), atom("signal"), atom("terminate"));
      },
      on(atom("shutdown")) >> [=]
      {
        // Make sure that signal watcher has terminated. We won't process the
        // DOWN message, though, since we quite immediately.
        detail::terminate();
        quit();
        LOG(verbose, core) << "system monitor @" << id() << " terminated";
      });
}

} // namespace vast
