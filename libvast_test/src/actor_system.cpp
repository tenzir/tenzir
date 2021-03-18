// SPDX-FileCopyrightText: (c) 2018 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "fixtures/actor_system.hpp"

#include "vast/fwd.hpp"

#include "vast/detail/assert.hpp"

#include <caf/io/middleman.hpp>

#include <filesystem>

namespace fixtures {

/// Configures the actor system of a fixture with default settings for unit
/// testing.
test_configuration::test_configuration() {
  std::string log_file = "vast-unit-test.log";
  set("logger.file-name", log_file);
  // Always begin with an empy log file.
  if (vast::exists(log_file))
    std::filesystem::remove_all(std::filesystem::path{log_file});
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
    std::filesystem::remove_all(std::filesystem::path{directory.str()});
}

actor_system::~actor_system() {
  // nop
}

deterministic_actor_system::deterministic_actor_system() {
  // Clean up state from previous executions.
  if (vast::exists(directory))
    std::filesystem::remove_all(std::filesystem::path{directory.str()});
}

} // namespace fixtures

