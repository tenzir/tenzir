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

#include "vast/detail/assert.hpp"
#include "vast/fwd.hpp"

#include <caf/io/middleman.hpp>

namespace fixtures {

/// Configures the actor system of a fixture with default settings for unit
/// testing.
test_configuration::test_configuration() {
  std::string log_file = "vast-unit-test.log";
  set("logger.file-name", log_file);
  // Always begin with an empy log file.
  if (vast::exists(log_file))
    vast::rm(log_file);
}

caf::error test_configuration::parse(int argc, char** argv) {
  auto err = super::parse(argc, argv);
  if (!err)
    set("logger.file-verbosity", caf::atom("trace"));
  return err;
}

/// A fixture with an actor system that uses the default work-stealing
/// scheduler.
actor_system::actor_system() : sys(config), self(sys, true) {
  // Clean up state from previous executions.
  if (vast::exists(directory))
    vast::rm(directory);
}

actor_system::~actor_system() {
  // nop
}

deterministic_actor_system::deterministic_actor_system() {
  // Clean up state from previous executions.
  if (vast::exists(directory))
    vast::rm(directory);
}

} // namespace fixtures

