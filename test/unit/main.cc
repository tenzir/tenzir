#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "vast/announce.h"
#include "vast/detail/adjust_resource_consumption.h"

int main(int argc, char** argv) {
  if (!vast::detail::adjust_resource_consumption())
    return 1;
  vast::announce_types();
  return caf::test::main(argc, argv);
}
