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

#pragma once

#include <caf/all.hpp>
#include <caf/test/dsl.hpp>

#include "vast/system/atoms.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/profiler.hpp"

#include "vast/detail/assert.hpp"

#include "test.hpp"
#include "fixtures/filesystem.hpp"

namespace fixtures {

/// Configures the actor system of a fixture with default settings for unit
/// testing.
struct test_configuration : vast::system::configuration {
  using super = vast::system::configuration;

  test_configuration(bool enable_mm = true) : super(enable_mm) {
    logger_file_name = "vast-unit-test.log";
    logger_component_filter.clear();
    // Always begin with an empy log file.
    if (vast::exists(logger_file_name))
      vast::rm(logger_file_name);
  }
};

/// A fixture with an actor system that uses the default work-stealing
/// scheduler.
struct actor_system : filesystem {
  actor_system(bool enable_mm = true)
    : config(enable_mm),
      system(config),
      self(system, true) {
    // Clean up state from previous executions.
    if (vast::exists(directory))
      vast::rm(directory);
    // Start profiler.
    if (vast::test::config.count("gperftools") > 0)
      enable_profiler();
  }

  ~actor_system() {
    // Stop profiler.
    using vast::system::stop_atom;
    using vast::system::heap_atom;
    using vast::system::cpu_atom;
    if (profiler) {
      self->send(profiler, stop_atom::value, cpu_atom::value);
      self->send(profiler, stop_atom::value, heap_atom::value);
    }
  }

  void enable_profiler() {
    VAST_ASSERT(!profiler);
    using vast::system::start_atom;
    using vast::system::heap_atom;
    using vast::system::cpu_atom;
    profiler = self->spawn(vast::system::profiler, directory / "profiler",
                           std::chrono::seconds(1));
    self->send(profiler, start_atom::value, cpu_atom::value);
    self->send(profiler, start_atom::value, heap_atom::value);
  }

  auto error_handler() {
    return [&](const caf::error& e) { FAIL(system.render(e)); };
  }

  test_configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
  caf::actor profiler;
};

/// A fixture with an actor system that uses the test coordinator for
/// determinstic testing of actors.
struct deterministic_actor_system
  : test_coordinator_fixture<test_configuration>,
    filesystem {

  using super = test_coordinator_fixture<test_configuration>;

  deterministic_actor_system(bool enable_mm = true) : super(enable_mm) {
    // Clean up state from previous executions.
    if (vast::exists(directory))
      vast::rm(directory);
  }

  auto error_handler() {
    return [&](const caf::error& e) { FAIL(sys.render(e)); };
  }
};

} // namespace fixtures

