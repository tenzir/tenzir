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

TEST(show first device) {
  if (auto first = self->system().opencl_manager().find_device())
    MESSAGE("found GPU: " << (*first)->name());
}

FIXTURE_SCOPE_END()
