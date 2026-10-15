//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/result.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/option.hpp"
#include "tenzir/unit.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Coroutine.h>
#include <folly/coro/WithCancellation.h>

#include <utility>

namespace tenzir {

/// A flag that asks for a stop on the stopped task's own terms: the slow path,
/// apart from cancellation.
///
/// Cancellation is the fast path, and stops a task at once. A stop that takes
/// its time, to drain connections, flush, or publish what is pending, is asked
/// for with a `StopFlag` instead. Taking one as a parameter says that the
/// function has such a slow path.
///
/// Not only polled: `wait()` completes once the stop is requested, and
/// `run(work)` races work against it. Copies share their state: requesting the
/// stop on one requests it on all, and every waiter sees it. A requested stop
/// stays requested.
class StopFlag {
public:
  StopFlag() : state_{std::in_place} {
  }

  /// Requests the stop, on this and on every child.
  auto request() const -> void {
    state_->source.requestCancellation();
  }

  /// Whether the stop has been requested.
  auto is_requested() const -> bool {
    return state_->source.isCancellationRequested();
  }

  /// Completes once the stop is requested, or at once if it was already.
  ///
  /// Cancelling this cancels the wait, and propagates.
  auto wait() const -> Task<void> {
    return wait_for(state_->source.getToken());
  }

  /// A stop that is requested whenever this one is, but whose own request
  /// does not reach this one.
  auto child() const -> StopFlag {
    auto result = StopFlag{};
    result.state_->parent.emplace(state_->source.getToken(),
                                  [source
                                   = result.state_->source]() mutable noexcept {
                                    source.requestCancellation();
                                  });
    return result;
  }

  /// Requests the stop when the returned guard goes out of scope.
  [[nodiscard]] auto request_on_exit() const {
    return detail::scope_guard{[state = state_]() noexcept {
      state->source.requestCancellation();
    }};
  }

  /// Runs `awaitable` until it completes or the stop is requested.
  ///
  /// None when the stop came first: it cancels `awaitable` like any
  /// cancellation, but ends here instead of propagating. The caller's own
  /// cancellation still propagates, also when both happen, and so does a
  /// cancellation that `awaitable` brought about itself.
  template <class SemiAwaitable>
  auto run(SemiAwaitable&& awaitable) const
    -> Task<Option<VoidToUnit<folly::coro::semi_await_result_t<SemiAwaitable>>>> {
    return run_until(state_->source.getToken(),
                     std::forward<SemiAwaitable>(awaitable));
  }

private:
  struct State {
    folly::CancellationSource source;
    /// Forwards the parent's request, for a child.
    Option<folly::CancellationCallback> parent;
  };

  // Static and taking the token by value, so that the coroutine does not refer
  // to the `StopFlag` it was called on.
  static auto wait_for(folly::CancellationToken stop) -> Task<void> {
    auto const& cancelled = co_await folly::coro::co_current_cancellation_token;
    co_await catch_cancellation(folly::coro::co_withCancellation(
      folly::cancellation_token_merge(cancelled, std::move(stop)),
      wait_forever()));
    co_await folly::coro::co_safe_point;
  }

  template <class SemiAwaitable>
  static auto run_until(folly::CancellationToken stop, SemiAwaitable awaitable)
    -> Task<Option<VoidToUnit<folly::coro::semi_await_result_t<SemiAwaitable>>>> {
    auto const& cancelled = co_await folly::coro::co_current_cancellation_token;
    auto result = co_await catch_cancellation(folly::coro::co_withCancellation(
      folly::cancellation_token_merge(cancelled, stop), std::move(awaitable)));
    co_await folly::coro::co_safe_point;
    // Cancelled, but neither by the caller nor by the stop: the work cancelled
    // itself, which is not a stop and goes on up.
    if (not result and not stop.isCancellationRequested()) {
      co_yield folly::coro::co_stopped_may_throw;
    }
    co_return result;
  }

  Arc<State> state_;
};

} // namespace tenzir
