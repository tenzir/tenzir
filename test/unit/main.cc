#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "vast/announce.h"
#include "vast/config.h"

#ifdef VAST_MACOS
#include <sys/resource.h> // setrlimit
#endif

namespace {

bool adjust_resource_limits()
{
#ifdef VAST_MACOS
  auto rl = ::rlimit{4096, 8192};
  auto result = ::setrlimit(RLIMIT_NOFILE,  &rl);
  return result == 0;
#endif
  return true;
}

} // namespace <anonymous>

int main(int argc, char** argv)
{
  if (! adjust_resource_limits())
    return 1;
  vast::announce_types();
  return caf::test::main(argc, argv);
}
