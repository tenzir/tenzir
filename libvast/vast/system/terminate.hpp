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
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/system/terminator.hpp"

#include <caf/actor_cast.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <vector>

namespace vast::policy {

struct sequential;
struct parallel;

} // namespace vast::policy

namespace vast::system {

/// Performs an asynchronous shutdown of a set of dependent actors,
/// configurable either in sequential or parallel mode of operation. As soon as
/// all dependent actors have terminated, the returned promise gets fulfilled.
/// @param self The actor to terminate.
/// @param xs Owned actors by *self* that need to shutdown prior to *self*.
/// @returns A response promise to be fulfilled when all *xs* terminated.
template <class Policy>
auto terminate(caf::event_based_actor* self, std::vector<caf::actor> xs) {
  auto timeout = defaults::system::shutdown_timeout;
  return self->request(self->spawn(terminator<Policy>), timeout, std::move(xs));
}

template <class Policy, class... Ts>
auto terminate(caf::typed_event_based_actor<Ts...>* self,
               std::vector<caf::actor> xs) {
  auto handle = caf::actor_cast<caf::event_based_actor*>(self);
  return terminate<Policy>(handle, std::move(xs));
}

template <class Policy>
void terminate(caf::scoped_actor& self, std::vector<caf::actor> xs) {
  auto timeout = defaults::system::shutdown_timeout;
  self->request(self->spawn(terminator<Policy>), timeout, std::move(xs))
    .receive(
      [=](atom::done) { VAST_DEBUG_ANON("terminated all dependent actor"); },
      [=](const caf::error& err) {
        VAST_ERROR_ANON("failed to terminated all dependent actors", err);
      });
}

} // namespace vast::system
