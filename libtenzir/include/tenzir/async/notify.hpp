//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/atomic.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>

namespace tenzir {

/// A multi-use notification primitive for coroutines.
///
/// Cancellation contract: A cancelled `wait()` never consumes a notification.
///
/// Wakeup contract: Each notification wakes exactly one `wait()`, even if
/// multiple waiters are suspended. When a state change must be observed by
/// all waiters (for example, a closed flag), each woken waiter has to cascade
/// by calling `notify_one()` before returning.
class Notify {
public:
  Notify() = default;
  ~Notify() = default;
  Notify(Notify const&) = delete;
  auto operator=(Notify const&) -> Notify& = delete;
  Notify(Notify&& other) noexcept
    : notified_{other.notified_.exchange(false, std::memory_order_relaxed)} {
  }
  auto operator=(Notify&& other) noexcept -> Notify& {
    notified_.store(other.notified_.exchange(false, std::memory_order_relaxed),
                    std::memory_order_relaxed);
    baton_.reset();
    return *this;
  }

  /// Wake up a single call to `wait()`, either now or later.
  ///
  /// Multiple calls to this function don't stack.
  void notify_one() {
    notified_.store(true, std::memory_order_release);
    baton_.post();
  }

  /// Wait for a notification. Returns immediately if already notified.
  ///
  /// If the wait is cancelled, it throws without consuming a notification.
  auto wait() -> Task<void> {
    auto& token = co_await folly::coro::co_current_cancellation_token;
    // The loop is needed because a posted baton might get left behind in a
    // previous iteration.
    while (true) {
      if (token.isCancellationRequested()) {
        co_yield folly::coro::co_stopped_may_throw;
      }
      if (notified_.exchange(false, std::memory_order_acquire)) {
        co_return;
      }
      // Block until `notify_one()` posts the baton, or the cancellation
      // callback posts it to unblock us
      auto callback = folly::CancellationCallback{token, [this]() noexcept {
                                                    baton_.post();
                                                  }};
      co_await baton_;
      baton_.reset();
    }
  }

private:
  Atomic<bool> notified_ = false;
  folly::coro::Baton baton_;
};

} // namespace tenzir
