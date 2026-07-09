//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/CurrentExecutor.h>

#include <deque>
#include <mutex>
#include <utility>

namespace tenzir {

/// A multi-use notification primitive for coroutines.
///
/// Wakeup contract: Each notification wakes exactly one `wait()`, in FIFO
/// order. Notifications don't stack: at most one is stored while no waiter is
/// suspended. When a state change must be observed by all waiters (for
/// example, a closed flag), each woken waiter has to cascade by calling
/// `notify_one()` before returning.
///
/// Cancellation contract: A cancelled `wait()` never consumes a notification.
/// If a notification was already assigned to a waiter that got cancelled, the
/// waiter hands it to the next waiter before throwing, or stores it if there
/// is none.
class Notify {
public:
  Notify() = default;

  ~Notify() {
    TENZIR_ASSERT(waiters_.empty());
  }

  Notify(Notify const&) = delete;
  auto operator=(Notify const&) -> Notify& = delete;

  /// Moving is only allowed while no `wait()` is in flight.
  Notify(Notify&& other) noexcept
    : notified_{std::exchange(other.notified_, false)} {
    TENZIR_ASSERT(other.waiters_.empty());
  }

  auto operator=(Notify&& other) noexcept -> Notify& {
    TENZIR_ASSERT(waiters_.empty());
    TENZIR_ASSERT(other.waiters_.empty());
    notified_ = std::exchange(other.notified_, false);
    return *this;
  }

  /// Wake up a single call to `wait()`, either now or later.
  ///
  /// Multiple calls to this function don't stack.
  void notify_one() {
    auto waiter = Option<Arc<Waiter>>{};
    {
      auto lock = std::scoped_lock{mutex_};
      waiter = deliver_one();
    }
    if (waiter.is_some()) {
      (*waiter)->baton.post();
    }
  }

  /// Wait for a notification. Returns immediately if already notified.
  ///
  /// If the wait is cancelled, it throws without consuming a notification.
  auto wait() -> Task<void> {
    auto& token = co_await folly::coro::co_current_cancellation_token;
    if (token.isCancellationRequested()) {
      co_yield folly::coro::co_stopped_may_throw;
    }
    auto waiter = Arc<Waiter>{std::in_place};
    {
      auto lock = std::scoped_lock{mutex_};
      if (notified_) {
        notified_ = false;
        co_return;
      }
      waiters_.push_back(waiter);
    }
    {
      // The callback holds its own reference so that a late `post()` cannot
      // touch a destroyed waiter.
      auto callback
        = folly::CancellationCallback{token, [waiter]() mutable noexcept {
                                        waiter->baton.post();
                                      }};
      co_await waiter->baton;
      // The callback's destructor joins an in-flight invocation, so no other
      // thread touches `waiter->baton` beyond this point.
    }
    // Read the cancellation state exactly once so that the decision below is
    // consistent: either we consume the notification, or we hand it over.
    auto cancelled = token.isCancellationRequested();
    auto notified = false;
    auto handoff = Option<Arc<Waiter>>{};
    {
      auto lock = std::scoped_lock{mutex_};
      notified = waiter->notified;
      if (not notified) {
        // Our wakeup came from the cancellation callback. Deregister so that
        // no notification gets assigned to us anymore.
        auto count = std::erase_if(waiters_, [&](Arc<Waiter> const& other) {
          return &*other == &*waiter;
        });
        TENZIR_ASSERT(count == 1);
      } else if (cancelled) {
        // A notification was assigned to us, but we are cancelled: pass it to
        // the next waiter, or store it if there is none.
        handoff = deliver_one();
      }
    }
    if (handoff.is_some()) {
      (*handoff)->baton.post();
    }
    if (cancelled) {
      co_yield folly::coro::co_stopped_may_throw;
    }
    TENZIR_ASSERT(notified);
  }

private:
  struct Waiter {
    folly::coro::Baton baton;
    /// Whether a notification was assigned to this waiter. Guarded by the
    /// owning `Notify`'s `mutex_`.
    bool notified = false;
  };

  /// Assign a notification to the next waiter, or store it if there is none.
  /// Must be called while holding `mutex_`. The returned waiter must be posted
  /// after releasing the lock, as posting resumes the waiting coroutine.
  auto deliver_one() -> Option<Arc<Waiter>> {
    if (waiters_.empty()) {
      notified_ = true;
      return None{};
    }
    auto waiter = std::move(waiters_.front());
    waiters_.pop_front();
    waiter->notified = true;
    return Option{std::move(waiter)};
  }

  std::mutex mutex_;
  /// A stored notification that no waiter was assigned to yet.
  bool notified_ = false;
  /// Waiters in FIFO order.
  std::deque<Arc<Waiter>> waiters_;
};

} // namespace tenzir
