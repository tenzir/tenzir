//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/result.hpp"
#include "tenzir/async/scope.hpp"
#include "tenzir/async/unbounded_queue.hpp"
#include "tenzir/detail/scope_guard.hpp"

#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace tenzir {

/// A dynamic set of tasks that yields results in completion order.
///
/// This is useful when a coroutine needs to start additional work over time and
/// then consume completed results one by one as they become available.
///
/// This type is not thread-safe. Calls to `add()` and `next()` must be
/// serialized by the caller and may only happen while the set is active.
template <class T>
class JoinSet {
public:
  /// Activate the set for the duration of a task.
  ///
  /// The given task may call `add()` and `next()`. This function only returns
  /// after the task itself and all tasks added to the set have terminated. Any
  /// tasks remaining after the given main task has terminated are cancelled.
  template <class U>
  auto activate(Task<U> task) -> Task<U> {
    auto guard = detail::scope_guard{[&] noexcept {
      // We reset the scope _after_ joining it.
      scope_ = nullptr;
    }};
    auto body = [&](AsyncScope& scope) -> Task<U> {
      scope_ = &scope;
      auto cancel_guard = detail::scope_guard{[&] noexcept {
        // We assume that the consumer is not interested anymore. Otherwise,
        // they would have called `next()` until it returns `None`.
        scope.cancel();
      }};
      co_return co_await std::move(task);
    };
    auto result = co_await void_to_unit(async_scope(std::move(body)));
    // Drain unconsumed completions so a subsequent activate() starts clean.
    while (running_ > 0) {
      co_await queue_.dequeue();
      running_ -= 1;
    }
    co_return unit_to_void(std::move(result));
  }

  /// Like `activate(Task<U>)`, but takes a function returning an awaitable.
  template <class F>
  auto activate(F&& f)
    -> Task<folly::coro::semi_await_result_t<std::invoke_result_t<F>>> {
    return activate(folly::coro::co_invoke(std::forward<F>(f)));
  }

  /// Return the next completed result, or `None` if no tasks remain.
  ///
  /// This is NOT thread safe and must only be used within `activate`.
  auto next() -> Task<Option<T>> {
    while (running_ > 0) {
      auto item = co_await queue_.dequeue();
      running_ -= 1;
      // A cancelled task does not return a result.
      if (item) {
        co_return item;
      }
    }
    co_return None{};
  }

  /// Add a new task to the set.
  ///
  /// If the task is cancelled, it contributes no value to `next()`. This
  /// function is NOT thread safe and must only be used within `activate`.
  template <class U>
    requires std::convertible_to<U, T>
  auto add(Task<U> task) -> void {
    TENZIR_ASSERT(scope_);
    running_ += 1;
    scope_->spawn([this, task = std::move(task)] mutable -> Task<void> {
      // Propagate cancellation as `None` to wake up `next()`.
      queue_.enqueue(co_await catch_cancellation(std::move(task)));
    });
  }

  /// Like `add(Task<U>)`, but takes a function returning an awaitable.
  template <class F>
    requires std::convertible_to<
      folly::coro::semi_await_result_t<std::invoke_result_t<F>>, T>
  auto add(F&& f) -> void {
    add(folly::coro::co_invoke(std::forward<F>(f)));
  }

  /// Returns the number of tasks that have not completed yet.
  ///
  /// This is NOT thread-safe and may only be used within `activate`.
  auto running() const -> size_t {
    return running_;
  }

private:
  size_t running_{0};
  UnboundedQueue<Option<T>> queue_;
  AsyncScope* scope_ = nullptr;
};

} // namespace tenzir
