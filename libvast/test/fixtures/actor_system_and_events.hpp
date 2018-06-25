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

#include "fixtures/actor_system.hpp"
#include "fixtures/events.hpp"

namespace fixtures {

/// A fixture with an actor system that uses the default work-stealing
/// scheduler and test data (events).
struct actor_system_and_events : actor_system, events {
  template <class... Ts>
  actor_system_and_events(Ts&&... xs) : actor_system(std::forward<Ts>(xs)...) {
    // nop
  }
};

/// A fixture with an actor system that uses the test coordinator for
/// determinstic testing of actors and test data (events).
struct deterministic_actor_system_and_events : deterministic_actor_system,
                                               events {
  template <class... Ts>
  deterministic_actor_system_and_events(Ts&&... xs)
    : deterministic_actor_system(std::forward<Ts>(xs)...) {
    // nop
  }
};

} // namespace fixtures

