//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>

namespace tenzir {

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
