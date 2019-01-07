/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "fixtures/actor_system.hpp"

#include <caf/io/middleman.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/profiler.hpp"

#include "vast/detail/assert.hpp"

namespace fixtures {

/// Configures the actor system of a fixture with default settings for unit
/// testing.
test_configuration::test_configuration() {
  load<caf::io::middleman>();
  std::string log_file = "vast-unit-test.log";
  set("logger.file-name", log_file);
  // Always begin with an empy log file.
  if (vast::exists(log_file))
    vast::rm(log_file);
}

/// A fixture with an actor system that uses the default work-stealing
/// scheduler.
actor_system::actor_system() : system(config), self(system, true) {
  // Clean up state from previous executions.
  if (vast::exists(directory))
    vast::rm(directory);
  // Start profiler.
  if (vast::test::config.count("gperftools") > 0)
    enable_profiler();
}

actor_system::~actor_system() {
  // Stop profiler.
  using vast::system::stop_atom;
  using vast::system::heap_atom;
  using vast::system::cpu_atom;
  if (profiler) {
    self->send(profiler, stop_atom::value, cpu_atom::value);
    self->send(profiler, stop_atom::value, heap_atom::value);
  }
}

void actor_system::enable_profiler() {
  VAST_ASSERT(!profiler);
  using vast::system::start_atom;
  using vast::system::heap_atom;
  using vast::system::cpu_atom;
  profiler = self->spawn(vast::system::profiler, directory / "profiler",
                         std::chrono::seconds(1));
  self->send(profiler, start_atom::value, cpu_atom::value);
  self->send(profiler, start_atom::value, heap_atom::value);
}

deterministic_actor_system::deterministic_actor_system() {
  // Clean up state from previous executions.
  if (vast::exists(directory))
    vast::rm(directory);
}

} // namespace fixtures

