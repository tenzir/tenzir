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
#include "vast/error.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"

#include <caf/behavior.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/response_promise.hpp>

namespace vast::system {

template <class Policy>
caf::behavior terminator(caf::stateful_actor<terminator_state>* self,
                         std::chrono::milliseconds grace_period,
                         std::chrono::milliseconds kill_timeout) {
  self->set_down_handler([=](const caf::down_msg& msg) {
    // Remove actor from list of remaining actors.
    VAST_DEBUG(self, "received DOWN from actor", msg.source);
    auto& remaining = self->state.remaining_actors;
    auto pred = [=](auto& actor) { return actor == msg.source; };
    auto i = std::find_if(remaining.begin(), remaining.end(), pred);
    VAST_ASSERT(i != remaining.end());
    remaining.erase(i);
    // Perform next action based on policy.
    if constexpr (std::is_same_v<Policy, policy::sequential>) {
      if (!remaining.empty()) {
        auto& next = remaining.back();
        VAST_DEBUG(self, "terminates next actor", next);
        self->monitor(next);
        self->send_exit(next, caf::exit_reason::user_shutdown);
        return;
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
  return {
    [=](const std::vector<caf::actor>& xs) {
      VAST_DEBUG(self, "got request to terminate", xs.size(), "actors");
      VAST_ASSERT(!self->state.promise.pending());
      self->state.promise = self->make_response_promise();
      auto& remaining = self->state.remaining_actors;
      remaining.reserve(xs.size());
      for (auto i = xs.rbegin(); i != xs.rend(); ++i)
        if (!*i)
          VAST_DEBUG(self,
                     "skips termination of already exited actor at position",
                     std::distance(xs.begin(), i.base()));
        else
          remaining.push_back(*i);
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
        // first actor to be terminated at the end. In sequential operation, we
        // monitor the next actor, send it an EXIT, wait for the DOWN, and then
        // move to the next. This ensures that we'll always process the DOWN
        // that corresponds to our EXIT message. (When monitoring an already
        // terminated actor, CAF dispatches the DOWN immediately.)
        auto& next = remaining.back();
        self->monitor(next);
        self->send_exit(next, caf::exit_reason::user_shutdown);
      } else if constexpr (std::is_same_v<Policy, policy::parallel>) {
        // Terminate all actors.
        for (auto& x : xs) {
          if (!x)
            continue;
          self->monitor(x);
          self->send_exit(x, caf::exit_reason::user_shutdown);
        }
      } else {
        static_assert(detail::always_false_v<Policy>, "unsupported policy");
      }
      // Send a reminder for killing all alive actors.
      if (grace_period > std::chrono::milliseconds::zero())
        self->delayed_send(self, grace_period, atom::shutdown_v);
    },
    [=](atom::shutdown) {
      VAST_ASSERT(!self->state.remaining_actors.empty());
      VAST_WARNING(self, "failed to terminate actors within grace period of",
                   grace_period);
      VAST_WARNING(self, "initiates hard kill of",
                   self->state.remaining_actors.size(), "remaining actors");
      // Kill remaining actors.
      for (auto& actor : self->state.remaining_actors) {
        VAST_DEBUG(self, "sends KILL to actor", actor->id());
        if constexpr (std::is_same_v<Policy, policy::sequential>)
          self->monitor(actor);
        self->send_exit(actor, caf::exit_reason::kill);
      }
      // Handle them now differently.
      self->set_down_handler([=](const caf::down_msg& msg) {
        VAST_DEBUG(self, "killed actor", msg.source.id());
        auto pred = [=](auto& actor) { return actor == msg.source; };
        auto& remaining = self->state.remaining_actors;
        auto i = std::find_if(remaining.begin(), remaining.end(), pred);
        if (i == remaining.end()) {
          VAST_DEBUG(self, "ignores duplicate DOWN message");
          return;
        }
        remaining.erase(i);
        if (remaining.empty()) {
          VAST_DEBUG(self, "killed all remaining actors");
          self->state.promise.deliver(atom::done_v);
          self->quit(caf::exit_reason::user_shutdown);
        }
      });
      // Send the final reminder for a hard-kill.
      if (kill_timeout > std::chrono::milliseconds::zero())
        self->delayed_send(self, kill_timeout, atom::stop_v);
      else
        self->send(self, atom::stop_v);
    },
    [=](atom::stop) {
      auto n = self->state.remaining_actors.size();
      VAST_ERROR(self, "failed to kill", n, "actors");
      self->state.promise.deliver(
        make_error(ec::timeout, "failed to kill remaining actors", n));
      self->quit(caf::exit_reason::user_shutdown);
    }};
}

template caf::behavior
terminator<policy::sequential>(caf::stateful_actor<terminator_state>*,
                               std::chrono::milliseconds,
                               std::chrono::milliseconds);

template caf::behavior
terminator<policy::parallel>(caf::stateful_actor<terminator_state>*,
                             std::chrono::milliseconds,
                             std::chrono::milliseconds);

} // namespace vast::system
