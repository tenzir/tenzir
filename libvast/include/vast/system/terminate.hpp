//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/defaults.hpp"
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

/// Performs an asynchronous shutdown of a set of actors by sending an EXIT
/// message, configurable either in sequential or parallel mode of operation.
/// As soon as all actors have terminated, the returned promise gets fulfilled.
/// This function is the lower-level interface for bringing down actors. The
/// function `shutdown` uses this function internally to implement a more
/// convenient one-stop solution.
/// @param self The actor to terminate.
/// @param xs The actors to terminate.
/// @returns A response handle that is replied to when the termination succeeded
/// or failed.
/// @relates shutdown
template <class Policy, class Actor>
[[nodiscard]] auto terminate(Actor&& self, std::vector<caf::actor> xs) {
  auto t = self->spawn(terminator<Policy>);
  return self->request(std::move(t), caf::infinite, std::move(xs));
}

template <class Policy, class Actor>
[[nodiscard]] auto terminate(Actor&& self, caf::actor x) {
  return terminate<Policy>(std::forward<Actor>(self),
                           std::vector{std::move(x)});
}

} // namespace vast::system
