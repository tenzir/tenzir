//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/scope.hpp"
#include "tenzir/async/unbounded_queue.hpp"

#include <folly/coro/AsyncGenerator.h>
#include <folly/coro/BoundedQueue.h>

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
  QueueScope() = default;
  ~QueueScope() noexcept = default;
  QueueScope(const QueueScope&) = delete;
  QueueScope& operator=(const QueueScope&) = delete;
  QueueScope(QueueScope&&) = delete;
  QueueScope& operator=(QueueScope&&) = delete;

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
      // TODO: Should we cancel the outstanding tasks here?
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
    requires std::convertible_to<U, T>
  void spawn(Task<U> task) {
    TENZIR_ASSERT(scope_);
    remaining_ += 1;
    scope_->spawn([this, task = std::move(task)] mutable -> Task<void> {
      co_await results_.enqueue(
        co_await folly::coro::co_awaitTry(std::move(task)));
    });
  }

  /// Spawn an asynchronous generator which populates the queue.
  ///
  /// The generator will only be advanced once the last item it produced is
  /// about to returned from `next()`.
  // TODO: This is not true yet.
  void spawn(folly::coro::AsyncGenerator<int> generator) {
    TENZIR_ASSERT(scope_);
    remaining_ += 1;
    scope_->spawn(
      [this, generator = std::move(generator)] mutable -> Task<void> {
        for (auto next : co_await generator.next()) {
          if (not next) {
            break;
          }
          remaining_ += 1;
          co_await results_.enqueue(std::move(*next));
        }
        // We still need to enqueue something to give `next` a chance to resume.
        co_await results_.enqueue(
          folly::make_exception_wrapper<folly::OperationCancelled>());
      });
  }

  template <class F>
    requires folly::coro::is_semi_awaitable_v<std::invoke_result_t<F>>
             and std::convertible_to<
               folly::coro::semi_await_result_t<std::invoke_result_t<F>>, T>
  void spawn(F&& f) {
    TENZIR_ASSERT(scope_);
    remaining_ += 1;
    scope_->spawn([this, f = std::forward<F>(f)] mutable -> Task<void> {
      co_await results_.enqueue(
        co_await folly::coro::co_awaitTry(std::invoke(std::move(f))));
    });
  }

  template <class U>
  struct IsAyncGenerator : std::false_type {};

  template <class U>
  struct IsAyncGenerator<folly::coro::AsyncGenerator<U>> : std::true_type {};

  template <class F>
    requires IsAyncGenerator<std::invoke_result_t<F>>::value
  void spawn(F&& f) {
    TENZIR_ASSERT(scope_);
    remaining_ += 1;
    scope_->spawn([this, f = std::move(f)] mutable -> Task<void> {
      auto generator = std::invoke(f);
      while (true) {
        auto result = co_await folly::coro::co_awaitTry(generator.next());
        TENZIR_ASSERT_ALWAYS(not result.hasException() or result.exception(),
                             "generator.next() returned empty exception "
                             "wrapper");
        if (result.hasException()) {
          co_await results_.enqueue(std::move(result).exception());
          co_return;
        }
        auto next = std::move(result).value();
        if (not next) {
          break;
        }
        remaining_ += 1;
        co_await results_.enqueue(std::move(*next));
      }
      // We still need to enqueue something to give `next` a chance to resume.
      co_await results_.enqueue(folly::exception_wrapper{});
    });
  }

  /// Cancel all remaining tasks.
  void cancel() {
    // TODO: Exact behavior.
    TENZIR_ASSERT(scope_);
    scope_->cancel();
  }

  auto is_cancelled() const -> bool {
    return scope_ != nullptr and scope_->is_cancelled();
  }

  using Next = std::conditional_t<std::is_same_v<T, void>, std::monostate, T>;

  /// Retrieve the next task result or return `nullopt` if none remain.
  ///
  /// This function can be called while the scope is active, but also when it
  /// already got deactivated. If a task failed, we rethrow the exception.
  /// TODO: What if it got cancelled?
  /// TODO: Is this itself cancel-safe?
  auto next() -> Task<std::optional<Next>> {
    while (remaining_ > 0) {
      auto result = co_await results_.dequeue();
      remaining_ -= 1;
      if (result.is_exception() and not result.exception()) {
        // An empty exception object is used to signal that a task has completed
        // that didn't produce a result.
        continue;
      }
      TENZIR_ASSERT_ALWAYS(not result.is_exception() or result.exception(),
                           "QueueScope::next() got empty exception wrapper");
      if constexpr (std::same_as<T, void>) {
        std::move(result).unwrap();
        co_return std::monostate{};
      } else {
        co_return std::move(result).unwrap();
      }
    }
    co_return std::nullopt;
  }

  auto scope() -> AsyncScope& {
    TENZIR_ASSERT(scope_);
    return *scope_;
  }

private:
  std::atomic<size_t> remaining_ = 0;
  folly::coro::BoundedQueue<AsyncResult<T>> results_{1};
  AsyncScope* scope_ = nullptr;
};

} // namespace tenzir
