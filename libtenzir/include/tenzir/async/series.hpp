//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/push_pull.hpp"
#include "tenzir/box.hpp"
#include "tenzir/series_builder.hpp"

#include <folly/coro/BoundedQueue.h>

namespace tenzir {

/// Single-slot channel for batch timeout durations.
///
/// Capacity 1 ensures at most one timer is queued at a time. Writing
/// replaces any stale pending duration so the consumer always wakes
/// with the most recent remaining wait.
using WaitChannel
  = folly::coro::BoundedQueue<std::chrono::steady_clock::duration>;

/// Creates a heap-allocated WaitChannel with the correct capacity.
inline auto new_wait_channel() -> Box<WaitChannel> {
  return Box<WaitChannel>{std::in_place, 1u};
}

/// Takes result of series_builder::yield_ready and pushes data or schedules
/// a timer via `wait_for` to call this function again after the timeout.
///
/// Importantly, this will not wait in this task, but will send the duration
/// that needs to be waited via the `wait_for` channel.
inline auto push_or_wait(series_builder::YieldReadyResult result,
                         Push<table_slice>& push, WaitChannel& wait_for)
  -> Task<void> {
  if (result.data) {
    co_await push(std::move(result.data.unwrap()));
  }
  if (result.wait_for) {
    // drop the stale duration (if any) so the consumer always wakes
    // with the freshest remaining wait
    wait_for.try_dequeue();
    wait_for.try_enqueue(result.wait_for.unwrap());
  }
}

} // namespace tenzir
