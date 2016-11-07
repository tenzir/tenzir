#include <caf/all.hpp>

#include "vast/system/configuration.hpp"

#include "test.hpp"

struct actor_system_fixture {
  actor_system_fixture() : system{config}, self{system} {
  }

  auto error_handler() {
    return [=](caf::error const& e) { FAIL(system.render(e)); };
  }

  vast::system::configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
};
