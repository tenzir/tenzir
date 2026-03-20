//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"

#include <folly/coro/Baton.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace tenzir {

/// A cancellation-safe, repeatable notification primitive for coroutines.
///
/// `notify_one()` is synchronous and safe to call from any context.
/// `wait()` suspends the calling coroutine until the next notification.
/// Supports cancellation: a cancelled `wait()` throws `OperationCancelled`.
class Notify {
public:
  void notify_one() {
    auto baton = std::shared_ptr<folly::coro::Baton>{};
    {
      auto lock = std::lock_guard{mutex_};
      notified_ = true;
      baton = baton_;
    }
    // Post outside the lock: post() may resume the waiter synchronously,
    // which would re-enter wait() and try to acquire the mutex.
    if (baton) {
      baton->post();
    }
  }

  auto wait() -> Task<void> {
    auto baton = std::shared_ptr<folly::coro::Baton>{};
    {
      auto lock = std::lock_guard{mutex_};
      if (notified_) {
        notified_ = false;
        co_return;
      }
      // Install a fresh baton for this wait.
      baton_ = std::make_shared<folly::coro::Baton>();
      baton = baton_;
    }
    // Wait outside the lock.
    co_await *baton;
    {
      auto lock = std::lock_guard{mutex_};
      notified_ = false;
      baton_.reset();
    }
  }

private:
  std::mutex mutex_;
  bool notified_ = false;
  std::shared_ptr<folly::coro::Baton> baton_;
};

} // namespace tenzir
