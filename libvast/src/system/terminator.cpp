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

#include "vast/system/terminator.hpp"

#include "vast/detail/assert.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/response_promise.hpp>

namespace vast::system {

template <class Policy>
caf::behavior terminator(caf::stateful_actor<terminator_state>* self) {
  self->set_down_handler([=](const caf::down_msg& msg) {
    // Remove actor from list of remaining actors.
    VAST_DEBUG(self, "received DOWN from actor", msg.source);
    auto& remaining = self->state.remaining_actors;
    auto i = std::find(remaining.begin(), remaining.end(), msg.source);
    VAST_ASSERT(i != remaining.end());
    remaining.erase(i);
    // Perform next action based on policy.
    if constexpr (std::is_same_v<Policy, policy::sequential>) {
      while (!remaining.empty()) {
        auto& x = remaining.back();
        if (auto next = caf::actor_cast<caf::actor>(x)) {
          VAST_DEBUG(self, "terminates next actor", next);
          self->monitor(next);
          self->send_exit(next, caf::exit_reason::user_shutdown);
          return;
        } else {
          VAST_DEBUG(self, "skips already exited actor");
          remaining.pop_back();
        }
      }
    } else if constexpr (std::is_same_v<Policy, policy::parallel>) {
      // nothing to do, all EXIT messages are in flight.
      VAST_DEBUG(self, "has", remaining.size(), "actors remaining");
    } else {
      static_assert(detail::always_false_v<Policy>, "unsupported policy");
    }
    if (remaining.empty()) {
      VAST_DEBUG(self, "terminated all actors");
      self->state.promise.deliver(atom::done_v);
      self->quit(caf::exit_reason::user_shutdown);
    }
  });
  return {[=](const std::vector<caf::actor>& xs) {
    VAST_DEBUG(self, "got request to terminate", xs.size(), "actors");
    VAST_ASSERT(!self->state.promise.pending());
    self->state.promise = self->make_response_promise();
    auto& remaining = self->state.remaining_actors;
    remaining.reserve(xs.size());
    for (auto i = xs.rbegin(); i != xs.rend(); ++i)
      if (!*i)
        VAST_DEBUG(self, "skips termination of already exited actor");
      else
        remaining.push_back((*i)->address());
    if (remaining.size() < xs.size())
      VAST_DEBUG(self, "only needs to terminate", remaining.size(), "actors");
    // Terminate early if there's nothing to do.
    if (remaining.empty()) {
      VAST_DEBUG(self, "quits prematurely because all actors have exited");
      self->state.promise.deliver(atom::done_v);
      self->quit(caf::exit_reason::user_shutdown);
      return;
    }
    if constexpr (std::is_same_v<Policy, policy::sequential>) {
      // Track actors in reverse order because the user provides the actors in
      // the order of shutdown, but we use a stack internally that stores the
      // first actor to be terminated at the end.
      auto& next = remaining.back();
      self->monitor(next);
      self->send_exit(next, caf::exit_reason::user_shutdown);
    } else if constexpr (std::is_same_v<Policy, policy::parallel>) {
      // Terminate all actors.
      for (auto& x : xs) {
        self->monitor(x);
        self->send_exit(x, caf::exit_reason::user_shutdown);
      }
    } else {
      static_assert(detail::always_false_v<Policy>, "unsupported policy");
    }
  }};
}

template caf::behavior
terminator<policy::sequential>(caf::stateful_actor<terminator_state>*);

template caf::behavior
terminator<policy::parallel>(caf::stateful_actor<terminator_state>*);

} // namespace vast::system
