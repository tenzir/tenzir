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

#include "vast/system/atoms.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/profiler.hpp"

#include "vast/detail/assert.hpp"

#include "test.hpp"
#include "fixtures/filesystem.hpp"

namespace fixtures {

struct actor_system : filesystem {
  struct configuration : vast::system::configuration {
    configuration() {
      logger_file_name = "vast-unit-test.log";
      logger_component_filter = "vast|caf";
      // Always begin with an empy log file.
      if (vast::exists(logger_file_name))
        vast::rm(logger_file_name);
    }
  };

  actor_system() : system{config}, self{system, true} {
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

  configuration config;
  caf::actor_system system;
  caf::scoped_actor self;
  caf::actor profiler;
};

} // namespace fixtures

