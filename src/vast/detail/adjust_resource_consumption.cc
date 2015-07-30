#include "vast/config.h"
#include "vast/detail/adjust_resource_consumption.h"

#ifdef VAST_MACOS
#include <sys/resource.h> // setrlimit
#endif

namespace vast {
namespace detail {

bool adjust_resource_consumption()
{
#ifdef VAST_MACOS
  auto rl = ::rlimit{4096, 8192};
  auto result = ::setrlimit(RLIMIT_NOFILE,  &rl);
  return result == 0;
#endif
  return true;
}

} // namespace detail
} // namespace vast
