#define CAF_TEST_NO_MAIN
#include <caf/test/unit_test_impl.hpp>

#include "vast/announce.h"

int main(int argc, char** argv)
{
  vast::announce_types();
  return caf::test::main(argc, argv);
}
