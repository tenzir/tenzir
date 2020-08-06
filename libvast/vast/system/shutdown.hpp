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

#include "vast/defaults.hpp"

#include <caf/actor_cast.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>

#include <vector>

namespace vast::policy {

struct sequential;
struct parallel;

} // namespace vast::policy

namespace vast::system {

/// Performs an asynchronous shutdown of a set of actors, followed by
/// terminating the actor in the calling context. The shutdown process runs
/// either sequentially or in parallel. As soon as all actors have terminated,
/// the calling actor exits. The shutdown process involves sending an EXIT
/// message with reason `user_shutdown`.
/// @param self The actor to terminate.
/// @param xs Actors that need to shutdown before *self* quits.
template <class Policy>
void shutdown(caf::event_based_actor* self, std::vector<caf::actor> xs,
              std::chrono::seconds shutdown_timeout
              = defaults::system::shutdown_timeout,
              std::chrono::seconds clean_exit_timeout
              = defaults::system::clean_exit_timeout,
              std::chrono::seconds kill_exit_timeout
              = defaults::system::kill_exit_timeout);

template <class Policy, class... Ts>
void shutdown(caf::typed_event_based_actor<Ts...>* self,
              std::vector<caf::actor> xs,
              std::chrono::seconds shutdown_timeout
              = defaults::system::shutdown_timeout,
              std::chrono::seconds clean_exit_timeout
              = defaults::system::clean_exit_timeout,
              std::chrono::seconds kill_exit_timeout
              = defaults::system::kill_exit_timeout) {
  auto handle = caf::actor_cast<caf::event_based_actor*>(self);
  shutdown<Policy>(handle, std::move(xs), shutdown_timeout, clean_exit_timeout,
                   kill_exit_timeout);
}

template <class Policy, class Actor>
void shutdown(Actor* self, caf::actor x,
              std::chrono::seconds shutdown_timeout
              = defaults::system::shutdown_timeout,
              std::chrono::seconds clean_exit_timeout
              = defaults::system::clean_exit_timeout,
              std::chrono::seconds kill_exit_timeout
              = defaults::system::kill_exit_timeout) {
  shutdown<Policy>(self, std::vector<caf::actor>{std::move(x)},
                   shutdown_timeout, clean_exit_timeout, kill_exit_timeout);
}

} // namespace vast::system
