//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <folly/coro/Task.h>

#include <chrono>

namespace tenzir {

template <class T>
using Task = folly::coro::Task<T>;

/// Returns a task that never completes (but can be cancelled).
auto wait_forever() -> Task<void>;

/// Returns a task that completes after the given duration.
auto sleep_for(std::chrono::steady_clock::duration d) -> Task<void>;

/// Returns a task that completes at the given point in time (or immediately if
/// it is already in the past).
auto sleep_until(std::chrono::steady_clock::time_point t) -> Task<void>;

/// Asserts that the current task is cancelled and propagates that cancellation.
auto assert_cancelled() -> Task<void>;

} // namespace tenzir
