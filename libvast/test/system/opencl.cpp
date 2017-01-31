#include <caf/all.hpp>
#include <caf/opencl/all.hpp>

#define SUITE opencl
#include "test.hpp"
#include "fixtures/actor_system.hpp"

using namespace caf;
using namespace vast;
using namespace vast::system;

FIXTURE_SCOPE(opencl_tests, fixtures::actor_system)

namespace {

// TODO: put functions with internal linkage here

} // namespace <anonymous>

TEST(non-empty first device) {
  auto& first = self->system().opencl_manager().get_device();
  REQUIRE(first);
  CHECK(!first->get_name().empty());
  std::cout << first->get_name() << std::endl;
}

FIXTURE_SCOPE_END()
