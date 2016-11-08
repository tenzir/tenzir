#include <caf/all.hpp>

#include "vast/system/configuration.hpp"

#include "test.hpp"

struct actor_system_fixture {
  struct configuration : vast::system::configuration {
    configuration() {
      logger_filename = "vast-unit-test.log";
    }
  };

  actor_system_fixture() : system{config}, self{system, true} {
  }

  auto error_handler() {
    return [=](caf::error const& e) { FAIL(system.render(e)); };
  }

  configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
};
