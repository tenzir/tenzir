#include <unistd.h>       // gethostname, getpid

#include <cerrno>

#include "vast/util/system.h"

namespace vast {
namespace util {

std::string hostname()
{
  char buf[256];
  if (::gethostname(buf, sizeof(buf)) == 0)
    return buf;
  //if (errno == EFAULT)
  //  VAST_ERROR("failed to get hostname: invalid address");
  //else if (errno == ENAMETOOLONG)
  //  VAST_ERROR("failed to get hostname: longer than 256 characters");
  return {};
}

int32_t process_id()
{
  return ::getpid();
}

} // namespace util
} // namespace vast
