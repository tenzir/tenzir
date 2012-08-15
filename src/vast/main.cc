#include <csignal>
#include <cstdlib>
#include "vast/program.h"

std::atomic<bool> terminating(false);
std::condition_variable cv;
struct sigaction old_sigint_handler;
struct sigaction old_sigterm_handler;

static void shutdown_handler(int signo)
{
  auto old = terminating.load();
  while (! terminating.compare_exchange_weak(old, !old))
    ;

  cv.notify_all();

  sigaction(SIGINT, &old_sigint_handler, NULL);
  sigaction(SIGTERM, &old_sigterm_handler, NULL);
}

int main(int argc, char *argv[])
{
  struct sigaction sig_handler;
  sig_handler.sa_handler = shutdown_handler;
  sigemptyset(&sig_handler.sa_mask);
  sig_handler.sa_flags = 0;
  sigaction(SIGINT, &sig_handler, &old_sigint_handler);
  sigaction(SIGTERM, &sig_handler, &old_sigterm_handler);

  vast::configuration config;
  if (! config.load(argc, argv))
    return EXIT_FAILURE;

  if (argc < 2 || config.check("help") || config.check("advanced"))
  {
    config.print(std::cerr, config.check("advanced"));
    return EXIT_SUCCESS;
  }

  vast::program program(config);
  program.start();

  std::mutex mutex;
  std::unique_lock<std::mutex> lock(mutex);
  cv.wait(lock, []() { return terminating.load() == true; });

  program.stop();

  return EXIT_SUCCESS;
}
