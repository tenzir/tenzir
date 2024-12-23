//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/weak_run_delayed.hpp>

#include <caf/response_promise.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir::detail {

// A helper actor for use within operators that want to sleep for a specific
// amount of time.
using alarm_clock_actor = caf::typed_actor<
  // Waits for` `delay` before returning.
  auto(duration delay)->caf::result<void>>;

inline auto make_alarm_clock(alarm_clock_actor::pointer self)
  -> alarm_clock_actor::behavior_type {
  return {
    [self](duration delay) -> caf::result<void> {
      auto rp = self->make_response_promise<void>();
      detail::weak_run_delayed(self, delay, [rp]() mutable {
        rp.deliver();
      });
      return rp;
    },
  };
}

} // namespace tenzir::detail
