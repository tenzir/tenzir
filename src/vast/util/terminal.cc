#include "vast/util/terminal.h"

#include <sys/select.h>
#include <stdio.h>
#include <termios.h>
#include <cassert>
#include <stdexcept>
#include "vast/util/poll.h"

namespace vast {
namespace util {
namespace terminal {

namespace {

bool initialized = false;
struct termios buffered;
struct termios unbuffered;

} // namespace <anonymous>

static void initialize()
{
  if (tcgetattr(0, &buffered) < 0)
    throw std::runtime_error{"tcgetattr"};
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
    throw std::runtime_error{"tcsetattr"};
}

void buffer()
{
  if (! initialized)
    return;
  if (tcsetattr(::fileno(stdin), TCSANOW, &buffered) < 0)
    throw std::runtime_error{"tcsetattr"};
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
