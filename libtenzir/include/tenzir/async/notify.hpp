//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/fibers/Semaphore.h>

namespace tenzir {

/// An async semaphore (that is movable unlike Folly's version).
class Semaphore {
public:
  explicit Semaphore(size_t initial) : impl_{initial} {
  }
  ~Semaphore() = default;
  Semaphore(Semaphore const&) = delete;
  auto operator=(Semaphore const&) -> Semaphore& = delete;
  Semaphore(Semaphore&& other) noexcept
    : impl_{other.impl_.getAvailableTokens()} {
  }
  auto operator=(Semaphore&& other) noexcept -> Semaphore& {
    // This assumes exclusive ownership over both semaphores.
    auto target = other.impl_.getAvailableTokens();
    auto current = impl_.getAvailableTokens();
    while (current < target) {
      impl_.signal();
      current += 1;
    }
    while (current > target) {
      // This is noexcept, but we still want to sanity check.
      TENZIR_ASSERT(impl_.try_wait());
      current -= 1;
    }
    TENZIR_ASSERT(impl_.getAvailableTokens()
                  == other.impl_.getAvailableTokens());
    return *this;
  }

  auto available_permits() const -> size_t {
    return impl_.getAvailableTokens();
  }

  auto signal() -> void {
    impl_.signal();
  }

  auto acquire() -> Task<void> {
    return impl_.co_wait();
  }

private:
  folly::fibers::Semaphore impl_{0};
};

/// A multi-use notification primitive for coroutines.
class Notify {
public:
  Notify() = default;
  ~Notify() = default;
  Notify(Notify const&) = delete;
  auto operator=(Notify const&) -> Notify& = delete;
  Notify(Notify&& other) noexcept : baton_{other.baton_.ready()} {
  }
  auto operator=(Notify&& other) noexcept -> Notify& {
    if (other.baton_.ready()) {
      baton_.post();
    } else {
      baton_.reset();
    }
    return *this;
  }

  /// Wake up a single call to `wait()`, either now or later.
  ///
  /// Multiple calls to this function don't stack.
  void notify_one() {
    baton_.post();
  }

  /// Wait for a notification. Returns immediately if already notified.
  auto wait() -> Task<void> {
    auto& token = co_await folly::coro::co_current_cancellation_token;
    auto callback = folly::CancellationCallback{token, [&]() noexcept {
                                                  baton_.post();
                                                }};
    co_await baton_;
    if (token.isCancellationRequested()) {
      co_yield folly::coro::co_cancelled;
    }
    // This races with other calls to `notify_one()`. However, that is okay, as
    // we only guarantee that the notification is eventually consumed. We can
    // thus pretend that we waited a bit longer here, the second notification
    // arrived (which gets ignored), and only then we consume the first one.
    baton_.reset();
  }

private:
  folly::coro::Baton baton_;
};

} // namespace tenzir
