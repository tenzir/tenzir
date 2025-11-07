//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/scope.hpp"

#include <folly/coro/UnboundedQueue.h>

namespace tenzir {

/// TODO:
/// This is a bit similar to `folly::coro::merge`, but we can't use that because
/// in our setup, some of the async generators would never finish. This means
/// that the merged generator does not finish. Thus, we have to destroy early,
/// and the docs warn against that:
/// > If the output stream is destroyed early (before reaching end-of-stream or
/// > exception), the remaining input generators are cancelled and detached;
/// > beware of use-after-free.

// TODO: Backpressure?
template <class T>
class QueueScope {
public:
  template <class U>
  auto activate(Task<U> task) -> Task<U> {
    co_return co_await async_scope([&](AsyncScope& scope) -> Task<U> {
      // We sneakily store the reference to the spawner here, making sure that
      // we reset it once we leave the async scope, as required for correctness.
      TENZIR_ASSERT(not scope_);
      scope_ = &scope;
      auto guard = detail::scope_guard{[&] noexcept {
        scope_ = nullptr;
      }};
      co_return co_await std::move(task);
      // TODO: What about the queue at this point?
    });
  }

  template <class F>
  auto activate(F&& f) -> Task<void> {
    // TODO: Fix typing.
    return activate(folly::coro::co_invoke(std::forward<F>(f)));
  }

  // TODO: Signature?
  // TODO: Thread-safety?
  template <class U>
  void spawn(Task<U> task) {
    TENZIR_ASSERT(scope_);
    scope_->spawn([this, task = std::move(task)] mutable -> Task<void> {
      queue_.enqueue(co_await folly::coro::co_awaitTry(std::move(task)));
    });
    remaining_ += 1;
  }

  template <class F>
    requires std::invocable<F>
  void spawn(F&& f) {
    TENZIR_ASSERT(scope_);
    scope_->spawn([this, f = std::forward<F>(f)] mutable -> Task<void> {
      queue_.enqueue(
        co_await folly::coro::co_awaitTry(std::invoke(std::move(f))));
    });
    remaining_ += 1;
  }

  /// Cancel all remaining tasks.
  void cancel() {
    // TODO: Exact behavior.
    TENZIR_ASSERT(scope_);
    scope_->cancel();
  }

  using Next = std::conditional_t<std::is_same_v<T, void>, std::monostate, T>;

  /// Retrieve the next task result or return `nullopt` if none remain.
  ///
  /// This function can be called while the scope is active, but also when it
  /// already got deactivated. If a task failed, we rethrow theexception.
  /// TODO: What if it got cancelled?
  /// TODO: Is this itself cancel-safe?
  auto next() -> Task<std::optional<Next>> {
    if (remaining_ == 0) {
      co_return {};
    }
    auto result = co_await queue_.dequeue();
    remaining_ -= 1;
    if constexpr (std::same_as<T, void>) {
      std::move(result).unwrap();
      co_return std::monostate{};
    } else {
      co_return std::move(result).unwrap();
    }
  }

private:
  std::atomic<size_t> remaining_ = 0;
  folly::coro::UnboundedQueue<AsyncResult<T>> queue_;
  AsyncScope* scope_ = nullptr;
};

} // namespace tenzir
