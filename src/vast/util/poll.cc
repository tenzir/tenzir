#include "vast/util/poll.h"

#include <cerrno>
#include <sys/select.h>
#include "vast/exception.h"

namespace vast {
namespace util {

bool poll(int fd, int usec)
{
  fd_set rdset;
  FD_ZERO(&rdset);
  FD_SET(fd, &rdset);
  struct timeval timeout{0, usec};
  auto rc = ::select(fd + 1, &rdset, nullptr, nullptr, &timeout);
  if (rc < 0)
  {
    switch (rc)
    {
      case EINTR:
      case ENOMEM:
        return false;
      default:
        throw exception("unhandled select() error");
    }
  }

  return FD_ISSET(fd, &rdset);
}

} // namespace util
} // namespace vast
