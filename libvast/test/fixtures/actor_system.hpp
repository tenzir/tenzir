#ifndef FIXTURES_ACTOR_SYSTEM_HPP
#define FIXTURES_ACTOR_SYSTEM_HPP

#include <caf/all.hpp>

#include "vast/filesystem.hpp"

#include "vast/system/configuration.hpp"

namespace fixtures {

struct actor_system {
  struct configuration : vast::system::configuration {
    configuration() {
      logger_filename = "vast-unit-test.log";
      // Always begin with an empy log file.
      if (vast::exists(logger_filename))
        vast::rm(logger_filename);
    }
  };

  actor_system() : system{config}, self{system, true} {
  }

  auto error_handler() {
    return [&](caf::error const& e) { FAIL(system.render(e)); };
  }

  vast::system::configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
};

} // namespace fixtures

#endif
