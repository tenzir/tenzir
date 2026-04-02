//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/series_builder.hpp"

namespace tenzir {

using WaitChannel = UnboundedQueue<std::chrono::steady_clock::duration>;

/// Takes result of series_builder::yield_ready and pushes data or triggers
/// a wait for timeout that should call this function again.
/// Importantly, this will not wait in this task, but will send the duration
/// that needs to be waited via the `wait_for` channel.
inline auto
push_or_wait(series_builder::YieldReadyResult result, Push<table_slice>& push,
             UnboundedQueue<std::chrono::steady_clock::duration>& wait_for)
  -> Task<void> {
  if (result.data) {
    co_await push(std::move(result.data.unwrap()));
  }
  if (result.wait_for) {
    wait_for.enqueue(result.wait_for.unwrap());
  }
}

} // namespace tenzir
