//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/defaults.hpp"

#include <caf/actor_cast.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/fwd.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <vector>

namespace tenzir::policy {

struct sequential;
struct parallel;

} // namespace tenzir::policy

namespace tenzir {

/// Performs an asynchronous shutdown of a set of actors, or terminates the
/// current process if that is not possible. The shutdown process runs
/// either sequentially or in parallel, based on the provided policy parameter.
/// This involves monitoring the actor, sending an EXIT message with reason
/// `user_shutdown`, and then waiting for the DOWN. As soon as all actors have
/// terminated, the calling actor exits with `caf::exit_reason::user_shutdown`.
/// If these failure semantics do not suit your use case, consider using the
/// function `terminate`, which allows for more detailed control over the
/// shutdown sequence.
/// @param self The actor to terminate.
/// @param xs Actors that need to shutdown before *self* quits.
/// @param reason The error message to terminate with; defaults to
/// caf::exit_reason::user_shutdown.
/// @relates terminate
template <class Policy>
void shutdown(caf::event_based_actor* self, std::vector<caf::actor> xs,
              caf::error reason = caf::exit_reason::user_shutdown);

template <class Policy, class... Ts>
void shutdown(caf::typed_event_based_actor<Ts...>* self,
              std::vector<caf::actor> xs,
              caf::error reason = caf::exit_reason::user_shutdown) {
  auto handle = caf::actor_cast<caf::event_based_actor*>(self);
  shutdown<Policy>(handle, std::move(xs), std::move(reason));
}

template <class Policy, class... Ts, class... Us>
void shutdown(caf::typed_event_based_actor<Ts...>* self,
              std::vector<caf::typed_actor<Us...>> xs,
              caf::error reason = caf::exit_reason::user_shutdown) {
  auto v = std::vector<caf::actor>{};
  v.reserve(xs.size());
  for (auto& x : xs) {
    v.emplace_back(caf::actor_cast<caf::actor>(std::move(x)));
  }
  auto handle = caf::actor_cast<caf::event_based_actor*>(self);
  shutdown<Policy>(handle, std::move(v), std::move(reason));
}

template <class Policy>
void shutdown(caf::scoped_actor& self, std::vector<caf::actor> xs,
              caf::error reason = caf::exit_reason::user_shutdown);

template <class Policy, class Actor>
void shutdown(Actor&& self, caf::actor x,
              caf::error reason = caf::exit_reason::user_shutdown) {
  shutdown<Policy>(std::forward<Actor>(self), std::vector{std::move(x)},
                   std::move(reason));
}

} // namespace tenzir
