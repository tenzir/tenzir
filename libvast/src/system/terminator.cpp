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

#include "vast/fwd.hpp"

#include "vast/atoms.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
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
    VAST_LOG_SPD_DEBUG("{} received DOWN from actor {}",
                       detail::id_or_name(self), msg.source);
    auto& remaining = self->state.remaining_actors;
    auto pred = [=](auto& actor) { return actor == msg.source; };
    auto i = std::find_if(remaining.begin(), remaining.end(), pred);
    VAST_ASSERT(i != remaining.end());
    remaining.erase(i);
    // Perform next action based on policy.
    if constexpr (std::is_same_v<Policy, policy::sequential>) {
      if (!remaining.empty()) {
        auto& next = remaining.back();
        VAST_LOG_SPD_DEBUG("{} terminates next actor {}",
                           detail::id_or_name(self), next);
        self->monitor(next);
        self->send_exit(next, caf::exit_reason::user_shutdown);
        return;
      }
    } else if constexpr (std::is_same_v<Policy, policy::parallel>) {
      // nothing to do, all EXIT messages are in flight.
      VAST_LOG_SPD_DEBUG("{} has {} actors remaining", detail::id_or_name(self),
                         remaining.size());
    } else {
      static_assert(detail::always_false_v<Policy>, "unsupported policy");
    }
    if (remaining.empty()) {
      VAST_LOG_SPD_DEBUG("{} terminated all actors", detail::id_or_name(self));
      self->state.promise.deliver(atom::done_v);
      self->quit(caf::exit_reason::user_shutdown);
    }
  });
  return {
    [=](const std::vector<caf::actor>& xs) {
      VAST_LOG_SPD_DEBUG("{} got request to terminate {} actors",
                         detail::id_or_name(self), xs.size());
      VAST_ASSERT(!self->state.promise.pending());
      self->state.promise = self->make_response_promise();
      auto& remaining = self->state.remaining_actors;
      remaining.reserve(xs.size());
      for (auto i = xs.rbegin(); i != xs.rend(); ++i)
        if (!*i)
          VAST_LOG_SPD_DEBUG(
            "{} skips termination of already exited actor at position {}",
            detail::id_or_name(self), std::distance(xs.begin(), i.base()));
        else
          remaining.push_back(*i);
      if (remaining.size() < xs.size())
        VAST_LOG_SPD_DEBUG("{} only needs to terminate {} actors",
                           detail::id_or_name(self), remaining.size());
      // Terminate early if there's nothing to do.
      if (remaining.empty()) {
        VAST_LOG_SPD_DEBUG("{} quits prematurely because all actors have "
                           "exited",
                           detail::id_or_name(self));
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
      VAST_LOG_SPD_WARN("{} failed to terminate actors within grace period of "
                        "{}",
                        detail::id_or_name(self), grace_period);
      VAST_LOG_SPD_WARN("{} initiates hard kill of {} remaining actors",
                        detail::id_or_name(self),
                        self->state.remaining_actors.size());
      // Kill remaining actors.
      for (auto& actor : self->state.remaining_actors) {
        VAST_LOG_SPD_DEBUG("{} sends KILL to actor {}",
                           detail::id_or_name(self), actor->id());
        if constexpr (std::is_same_v<Policy, policy::sequential>)
          self->monitor(actor);
        self->send_exit(actor, caf::exit_reason::kill);
      }
      // Handle them now differently.
      self->set_down_handler([=](const caf::down_msg& msg) {
        VAST_LOG_SPD_DEBUG("{} killed actor {}", detail::id_or_name(self),
                           msg.source.id());
        auto pred = [=](auto& actor) { return actor == msg.source; };
        auto& remaining = self->state.remaining_actors;
        auto i = std::find_if(remaining.begin(), remaining.end(), pred);
        if (i == remaining.end()) {
          VAST_LOG_SPD_DEBUG("{} ignores duplicate DOWN message",
                             detail::id_or_name(self));
          return;
        }
        remaining.erase(i);
        if (remaining.empty()) {
          VAST_LOG_SPD_DEBUG("{} killed all remaining actors",
                             detail::id_or_name(self));
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
      VAST_LOG_SPD_ERROR("{} failed to kill {} actors",
                         detail::id_or_name(self), n);
      self->state.promise.deliver(
        caf::make_error(ec::timeout, "failed to kill remaining actors", n));
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
