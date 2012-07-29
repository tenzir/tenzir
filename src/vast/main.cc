#include <csignal>
#include <cstdlib>
#include "vast/program.h"

vast::program VAST;

static void shutdown_handler(int signo)
{
  // FIXME: Calling this function is not safe. On POSIX,
  // use sem_post and sem_wait in conjunction with a watchdog actor.
  VAST.stop();
}

int main(int argc, char *argv[])
{
  struct sigaction sig_handler;
  sig_handler.sa_handler = shutdown_handler;
  sigemptyset(&sig_handler.sa_mask);
  sig_handler.sa_flags = 0;

  sigaction(SIGINT, &sig_handler, NULL);
  sigaction(SIGHUP, &sig_handler, NULL);
  sigaction(SIGTERM, &sig_handler, NULL);

  if (VAST.init(argc, argv))
    VAST.start();
  else
    return EXIT_FAILURE;

  return VAST.end();
}
