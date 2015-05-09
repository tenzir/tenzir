#include "vast/util/terminal.h"

#include <sys/select.h>
#include <stdio.h>
#include <termios.h>
#include <unistd.h>
#include <cstdlib>
#include "vast/util/posix.h"

namespace vast {
namespace util {
namespace terminal {

namespace {

bool initialized = false;
struct termios backup;
struct termios current;

void restore()
{
  if (initialized)
    tcsetattr(::fileno(stdin), TCSANOW, &backup);
}

bool initialize()
{
  if (! ::isatty(::fileno(stdin)))
    return false;
  std::atexit(&restore);
  if (initialized)
    return false;
  if (tcgetattr(0, &current) < 0 || tcgetattr(0, &backup) < 0)
    return false;
  initialized = true;
  return true;
}

} // namespace <anonymous>

unbufferer::unbufferer()
{
  unbuffer();
}

unbufferer::~unbufferer()
{
  buffer();
}

bool unbuffer()
{
  if (! (initialized || initialize()))
    return false;
  current.c_lflag &= ~(ICANON | ECHO);
  current.c_cc[VMIN] = 1;
  current.c_cc[VTIME] = 0;
  return true;
}

bool buffer()
{
  if (! (initialized || initialize()))
    return false;
  current.c_lflag |= ICANON | ECHO;
  current.c_cc[VMIN] = backup.c_cc[VMIN];
  current.c_cc[VTIME] = backup.c_cc[VTIME];
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool disable_echo()
{
  if (! (initialized || initialize()))
    return false;
  current.c_lflag &= ~ECHO;
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool enable_echo()
{
  if (! (initialized || initialize()))
    return false;
  current.c_lflag |= ECHO;
  return tcsetattr(::fileno(stdin), TCSANOW, &current) < 0;
}

bool get(char& c, int timeout)
{
  if (! poll(::fileno(stdin), timeout))
    return false;
  auto i = ::fgetc(stdin);
  if (::feof(stdin))
    return false;
  c = static_cast<char>(i);
  return true;
}

} // namespace terminal
} // namespace util
} // namespace vast
