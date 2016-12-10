#ifndef FIXTURES_ACTOR_SYSTEM_HPP
#define FIXTURES_ACTOR_SYSTEM_HPP

#include <caf/all.hpp>

#include "vast/filesystem.hpp"

#include "vast/system/atoms.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/profiler.hpp"

namespace fixtures {

struct actor_system {
  struct configuration : vast::system::configuration {
    configuration() {
      logger_filename = "vast-unit-test.log";
      logger_filter = "vast|caf";
      // Always begin with an empy log file.
      if (vast::exists(logger_filename))
        vast::rm(logger_filename);
    }
  };

  actor_system() : system{config}, self{system, true} {
    // Clean up state from previous executions.
    if (vast::exists(directory))
      vast::rm(directory);
    // Start profiler.
    using vast::system::start_atom;
    using vast::system::heap_atom;
    using vast::system::cpu_atom;
    profiler = self->spawn(vast::system::profiler, directory / "profiler",
                           std::chrono::seconds(1));
    self->send(profiler, start_atom::value, cpu_atom::value);
    self->send(profiler, start_atom::value, heap_atom::value);
  }

  ~actor_system() {
    // Stop profiler.
    using vast::system::stop_atom;
    using vast::system::heap_atom;
    using vast::system::cpu_atom;
    self->send(profiler, stop_atom::value, cpu_atom::value);
    self->send(profiler, stop_atom::value, heap_atom::value);
  }

  auto error_handler() {
    return [&](caf::error const& e) { FAIL(system.render(e)); };
  }

  configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
  caf::actor profiler;
  vast::path directory = "vast-unit-test";
};

} // namespace fixtures

#endif
