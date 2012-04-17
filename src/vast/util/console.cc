#include "vast/util/console.h"

#include <stdio.h>
#include <termios.h>
#include <cassert>
#include "vast/exception.h"

namespace vast {
namespace util {

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

    if (tcsetattr(0, TCSANOW, &unbuffered) < 0)
        throw exception("tcsetattr");
}

void buffer()
{
    assert(initialized);

    if (tcsetattr(0, TCSANOW, &buffered) < 0)
        throw exception("tcsetattr");
}

} // namespace util
} // namespace vast
