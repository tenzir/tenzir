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
#include <caf/ref_counted.hpp>
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
                      Function&& function) -> caf::disposable {
  return self->run_scheduled_weak(self->clock().now() + delay,
                                  std::forward<Function>(function));
}

struct weak_run_delayed_disposable_impl final : caf::ref_counted,
                                                caf::disposable::impl {
  void dispose() override {
    inner_.dispose();
  }

  bool disposed() const noexcept override {
    return inner_.disposed();
  }

  void ref_disposable() const noexcept override {
    ref();
  }

  void deref_disposable() const noexcept override {
    deref();
  }

  friend void
  intrusive_ptr_add_ref(const weak_run_delayed_disposable_impl* p) noexcept {
    p->ref();
  }

  friend void
  intrusive_ptr_release(const weak_run_delayed_disposable_impl* p) noexcept {
    p->deref();
  }

  caf::disposable inner_;
};

/// Runs an action in a loop with a given delay without keeping the actor alive.
///
/// The function is first called at `start`. Even if `start` is in the past, it
/// will be scheduled and not called immediately here.
template <class F>
auto weak_run_delayed_loop_at(caf::scheduled_actor* self,
                              caf::actor_clock::time_point start,
                              caf::timespan delay, F&& f) -> caf::disposable {
  // Using `weak_run_delayed` here would introduce clock drift.
  auto impl = caf::make_counted<weak_run_delayed_disposable_impl>();
  impl->inner_ = self->run_scheduled_weak(
    start, [self, impl, start, delay, f = std::forward<F>(f)]() mutable {
      std::invoke(f);
      impl->inner_
        = weak_run_delayed_loop_at(self, start + delay, delay, std::move(f));
    });
  return caf::disposable{std::move(impl)};
}

/// Runs an action in a loop with a given delay without keeping the actor alive.
/// @param self The hosting actor pointer.
/// @param delay The delay after which to repeat the action.
/// @param function The action to run with the signature () -> void.
/// @param run_immediately Whether to run the function immediately.
template <class Function>
  requires std::is_invocable_r_v<void, std::remove_reference_t<Function>&>
auto weak_run_delayed_loop(caf::scheduled_actor* self, caf::timespan delay,
                           Function&& function, bool run_immediately = true)
  -> caf::disposable {
  if (run_immediately) {
    std::invoke(function);
  }
  return weak_run_delayed_loop_at(self, self->clock().now() + delay, delay,
                                  std::forward<Function>(function));
}

} // namespace tenzir::detail
