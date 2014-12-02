#include "vast/util/system.h"

#include <unistd.h>  // gethostname
#include <cerrno>

namespace vast {
namespace util {

trial<std::string> hostname()
{
  char buf[256];
  auto r = ::gethostname(buf, sizeof(buf));
  if (r == 0)
    return std::string{buf};

  if (errno == EFAULT)
    return error{"invalid addres"};
  else if (errno == ENAMETOOLONG)
    return error{"hostname longer than 256 characters"};

  return error{"unknown error"};
}

} // namespace util
} // namespace vast
