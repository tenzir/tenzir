#include <vast/util/console.h>

#include <sys/select.h>
#include <stdio.h>
#include <termios.h>
#include <cassert>
#include <vast/exception.h>

namespace vast {
namespace util {
namespace console {

static bool initialized = false;
static struct termios buffered;
static struct termios unbuffered;

static void initialize()
{
  if (tcgetattr(0, &buffered) < 0)
    throw exception("tcgetattr");

  unbuffered = buffered;
  unbuffered.c_lflag &= (~ICANON & ~ECHO);
  unbuffered.c_cc[VMIN] = 1;
  unbuffered.c_cc[VTIME] = 0;

  initialized = true;
}

void unbuffer()
{
  if (! initialized)
    initialize();

  if (tcsetattr(::fileno(stdin), TCSANOW, &unbuffered) < 0)
    throw exception("tcsetattr");
}

void buffer()
{
  assert(initialized);

  if (tcsetattr(::fileno(stdin), TCSANOW, &buffered) < 0)
    throw exception("tcsetattr");
}

bool get(char& c, int timeout)
{
  fd_set rdset;
  FD_ZERO(&rdset);
  FD_SET(::fileno(stdin), &rdset);
  struct timeval tv{0, timeout};
  auto rc = ::select(::fileno(stdin) + 1, &rdset, nullptr, nullptr, &tv);
  if (rc <= 0)
    return false;

  if (! FD_ISSET(fileno(stdin), &rdset))
    return false;

  auto i = ::fgetc(stdin);
  if (::feof(stdin))
    return false;

  c = static_cast<char>(i);
  return true;
}

} // namespace console
} // namespace util
} // namespace vast
