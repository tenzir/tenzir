//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/option.hpp"
#include "tenzir/time.hpp"

#include <folly/coro/Collect.h>

#include <chrono>

namespace tenzir {

/// Coordinates a batch flush deadline with external completion wakeups so an
/// operator's `await_task()` wakes promptly when either an in-flight request
/// finishes or the current batch's timeout expires.
class FlushWakeup {
public:
  /// Arms the flush deadline if not already armed, waking `wait()`. Calling
  /// `arm()` while already armed is a no-op so adding more events to an open
  /// batch never pushes its deadline out.
  auto arm(duration timeout) -> void {
    if (deadline_) {
      return;
    }
    deadline_
      = std::chrono::steady_clock::now()
        + std::chrono::duration_cast<std::chrono::steady_clock::duration>(
          timeout);
    ready_->notify_one();
  }

  auto disarm() -> void {
    deadline_ = None{};
  }

  auto armed() const -> bool {
    return deadline_.has_value();
  }

  /// Returns whether the armed deadline has elapsed.
  ///
  /// A size-based flush can disarm and re-arm the deadline while an older
  /// timeout task is still sleeping, so timeout wakeups must check `due()`
  /// before flushing to avoid a double flush.
  auto due() const -> bool {
    return deadline_ and std::chrono::steady_clock::now() >= *deadline_;
  }

  /// Returns an owning wakeup callback suitable for `RequestWindow::spawn()`.
  auto wakeup() const {
    return [ready = ready_]() mutable {
      ready->notify_one();
    };
  }

  /// Waits until either a wakeup fires or the armed deadline expires.
  auto wait() const -> Task<void> {
    if (not deadline_) {
      co_await ready_->wait();
    }
    if (deadline_) {
      // Race the flush deadline against a wakeup, so that a completed request
      // is polled promptly instead of at the deadline. A cancelled wait never
      // consumes a notification, so losing the race drops no wakeup.
      co_await folly::coro::collectAnyNoDiscard(sleep_until(*deadline_),
                                                ready_->wait());
    }
  }

private:
  Option<std::chrono::steady_clock::time_point> deadline_ = None{};
  /// Shared with spawned request wakeups, which may outlive one drain.
  mutable Arc<Notify> ready_{std::in_place};
};

} // namespace tenzir
