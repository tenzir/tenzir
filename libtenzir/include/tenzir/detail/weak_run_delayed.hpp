//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include <caf/add_ref.hpp>
#include <caf/scheduled_actor.hpp>

namespace tenzir::detail {

/// Runs an action after a given delay without keeping the actor alive.
/// @param self The hosting actor pointer.
/// @param delay The delay after which to run the action.
/// @param function The action to run with the signature () -> void.
/// @returns A disposable that allows for cancelling the action.
/// See also: https://gitter.im/actor-framework/chat?at=63b03e24be2c3c20c727a443
template <class Function>
  requires std::is_invocable_r_v<void, Function&&>
auto weak_run_delayed(caf::scheduled_actor* self, caf::timespan delay,
                      Function&& function) {
  return self->clock().schedule(
    self->clock().now() + delay,
    caf::make_action(std::forward<Function>(function)),
    caf::weak_actor_ptr{self->ctrl(), caf::add_ref});
}

/// Runs an action in a loop with a given delay without keeping the actor alive.
///
/// The function is first called at `start`. Even if `start` is in the past, it
/// will be scheduled and not called immediately here.
template <class F>
void weak_run_delayed_loop_at(caf::scheduled_actor* self,
                              caf::actor_clock::time_point start,
                              caf::timespan delay, F&& f) {
  // Using `weak_run_delayed` here would introduce clock drift.
  self->clock().schedule(
    start,
    caf::make_action([self, start, delay, f = std::forward<F>(f)]() mutable {
      std::invoke(f);
      weak_run_delayed_loop_at(self, start + delay, delay, std::move(f));
    }),
    caf::weak_actor_ptr{self->ctrl(), caf::add_ref});
}

/// Runs an action in a loop with a given delay without keeping the actor alive.
/// @param self The hosting actor pointer.
/// @param delay The delay after which to repeat the action.
/// @param function The action to run with the signature () -> void.
/// @param run_immediately Whether to run the function immediately.
template <class Function>
  requires std::is_invocable_r_v<void, std::remove_reference_t<Function>&>
void weak_run_delayed_loop(caf::scheduled_actor* self, caf::timespan delay,
                           Function&& function, bool run_immediately = true) {
  if (run_immediately) {
    std::invoke(function);
  }
  weak_run_delayed_loop_at(self, self->clock().now() + delay, delay,
                           std::forward<Function>(function));
}

} // namespace tenzir::detail
