//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/test/fixtures/actor_system.hpp"
#include "vast/test/fixtures/events.hpp"

namespace fixtures {

/// A fixture with an actor system that uses the default work-stealing
/// scheduler and test data (events).
struct actor_system_and_events : actor_system, events {
  explicit actor_system_and_events(std::string_view suite)
    : actor_system(suite) {
  }
};

/// A fixture with an actor system that uses the test coordinator for
/// determinstic testing of actors and test data (events).
struct deterministic_actor_system_and_events : deterministic_actor_system,
                                               events {
  explicit deterministic_actor_system_and_events(std::string_view suite)
    : deterministic_actor_system(suite) {
  }
};

} // namespace fixtures
