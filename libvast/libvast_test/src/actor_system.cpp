//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
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
  std::filesystem::path log_file = "vast-unit-test.log";
  set("caf.logger.file.path", log_file.string());
  // Always begin with an empy log file.
  if (std::filesystem::exists(log_file))
    std::filesystem::remove_all(log_file);
}

caf::error test_configuration::parse(int argc, char** argv) {
  auto err = super::parse(argc, argv);
  if (!err)
    set("caf.logger.file.verbosity", "trace");
  return err;
}

/// A fixture with an actor system that uses the default work-stealing
/// scheduler.
actor_system::actor_system(std::string_view suite)
  : fixtures::filesystem(suite), sys(config), self(sys, true) {
  // Clean up state from previous executions.
  if (std::filesystem::exists(directory))
    std::filesystem::remove_all(directory);
}

actor_system::~actor_system() {
  // nop
}

deterministic_actor_system::deterministic_actor_system(std::string_view suite)
  : filesystem(suite) {
  // Clean up state from previous executions.
  if (std::filesystem::exists(directory))
    std::filesystem::remove_all(directory);
}

} // namespace fixtures
