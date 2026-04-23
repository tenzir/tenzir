//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/notify.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/option.hpp"
#include "tenzir/panic.hpp"

#include <mutex>
#include <utility>

namespace tenzir {

/// A write-once, read-once asynchronous value channel.
///
/// `send()` succeeds only on the first call. `recv()` may only be called once
/// and completes once a value was sent.
template <class T>
class Oneshot {
public:
  Oneshot() = default;

  auto send(T value) -> bool {
    if (done_.load(std::memory_order_acquire)) {
      return false;
    }
    {
      auto guard = std::scoped_lock{mutex_};
      if (done_.load(std::memory_order_relaxed)) {
        return false;
      }
      value_.emplace(std::move(value));
      done_.store(true, std::memory_order_release);
    }
    ready_.notify_one();
    return true;
  }

  auto has_sent() const -> bool {
    return done_.load(std::memory_order_acquire);
  }

  auto recv() -> Task<T> {
    auto expected = false;
    if (not received_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
      panic("Oneshot::recv() called more than once");
    }
    while (not done_.load(std::memory_order_acquire)) {
      co_await ready_.wait();
    }
    auto guard = std::scoped_lock{mutex_};
    if (not value_) {
      panic("Oneshot::recv() observed empty value");
    }
    co_return std::move(*value_);
  }

private:
  Atomic<bool> done_{false};
  Atomic<bool> received_{false};
  std::mutex mutex_;
  Option<T> value_{None{}};
  Notify ready_;
};

} // namespace tenzir
