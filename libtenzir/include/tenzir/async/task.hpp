//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/time.hpp>

#include <folly/coro/Task.h>

namespace tenzir {

template <class T>
using Task = folly::coro::Task<T>;

/// Returns a task that never completes (but can be cancelled).
auto wait_forever() -> Task<void>;

/// Returns a task that completes after the given duration.
auto sleep_for(duration d) -> Task<void>;

/// Returns a task that completes at the given point in time (or immediately if
/// it is already in the past).
auto sleep_until(time t) -> Task<void>;

} // namespace tenzir
