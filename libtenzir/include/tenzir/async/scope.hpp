//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/notify.hpp"
#include "tenzir/async/result.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/logger.hpp"

#include <folly/coro/AsyncScope.h>

namespace tenzir {

/// Handle to an asynchronous, scoped task that was spawned.
template <class T>
class AsyncHandle {
public:
  /// Wait for the associated task to complete and return its result.
  ///
  /// If a call to this function is cancelled, then the underlying task will not
  /// be joined and nothing happens. May be successfully awaited at most once.
  auto join() -> Task<AsyncResult<T>> {
    TENZIR_ASSERT(state_);
    TENZIR_ASSERT(not state_->notified);
    co_await state_->notify.wait();
    state_->notified = true;
    if constexpr (std::same_as<T, void>) {
      std::move(state_->value).unwrap();
      co_return {};
    } else {
      co_return std::move(state_->value).unwrap();
    }
  }

private:
  friend class AsyncScope;

  struct State {
    bool notified = false;
    Notify notify;
    AsyncResult<T> value;
  };

  explicit AsyncHandle(std::shared_ptr<State> state)
    : state_{std::move(state)} {
  }

  std::shared_ptr<State> state_;
};

/// Utility type created by `async_scope`.
class AsyncScope {
public:
  /// Spawn an awaitable, for example a task.
  ///
  /// The returned handle can be used to join the awaitable and returns its
  /// result. When dropped without joining, the awaitable continues running.
  template <class SemiAwaitable>
  auto spawn(SemiAwaitable&& awaitable)
    -> AsyncHandle<folly::coro::semi_await_result_t<SemiAwaitable>> {
    using Result = folly::coro::semi_await_result_t<SemiAwaitable>;
    auto state = std::make_shared<typename AsyncHandle<Result>::State>();
    scope_.add(
      folly::coro::co_withExecutor(
        executor_,
        folly::coro::co_invoke([state, awaitable = std::forward<SemiAwaitable>(
                                         awaitable)] mutable -> Task<void> {
          state->value
            = co_await folly::coro::co_awaitTry(std::move(awaitable));
          state->notify.notify_one();
        })),
      std::nullopt, FOLLY_ASYNC_STACK_RETURN_ADDRESS());
    return AsyncHandle<Result>{std::move(state)};
  }

  /// Spawn a function.
  template <class F>
  auto spawn(F&& f)
    -> AsyncHandle<folly::coro::semi_await_result_t<std::invoke_result_t<F>>> {
    // We need to `co_invoke` to ensure that the function itself is kept alive.
    return spawn(folly::coro::co_invoke(std::forward<F>(f)));
  }

  /// Cancel all remaining tasks.
  auto cancel() {
    // TODO: Exact behavior?
    scope_.requestCancellation();
  }

  auto is_cancelled() const -> bool {
    return scope_.isScopeCancellationRequested();
  }

private:
  AsyncScope(const AsyncScope&) = delete;
  auto operator=(const AsyncScope&) -> AsyncScope& = delete;
  AsyncScope(AsyncScope&&) = delete;
  auto operator=(AsyncScope&&) -> AsyncScope& = delete;

  AsyncScope(folly::ExecutorKeepAlive<> executor,
             folly::coro::CancellableAsyncScope& scope)
    : executor_{std::move(executor)}, scope_{scope} {
  }
  ~AsyncScope() = default;

  folly::ExecutorKeepAlive<> executor_;
  folly::coro::CancellableAsyncScope& scope_;

  template <class F>
    requires std::invocable<F, AsyncScope&>
  friend auto async_scope(F&& f) -> Task<
    folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>>;
};

/// Provides a scope that can spawn tasks for structured concurrency.
///
/// The given function and all tasks spawned may access external objects as long
/// as they outlive this function call. It will only return once all spawned
/// tasks have been completed. If the function fails or is cancelled, then all
/// spawned tasks will be cancelled.
///
/// Fun fact: This function is one of the very few things that would not be
/// possible in Rust, since implementing it requires async cancellation.
template <class F>
  requires std::invocable<F, AsyncScope&>
auto async_scope(F&& f) -> Task<
  folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>> {
  auto scope = folly::coro::CancellableAsyncScope{folly::CancellationToken{
    co_await folly::coro::co_current_cancellation_token}};
  auto spawn = AsyncScope{co_await folly::coro::co_current_executor, scope};
  // For memory safety, after calling the user-provided function, we must under
  // all circumstances reach the cleanup and join all spawned tasks, since they
  // may reference objects that might be destroyed once this function returns.
  // Although with Folly, we could rely on being able to continue as long as we
  // catch exceptions, the coroutine could in theory just be destroyed. Thus, we
  // use a normal scope guard to protect against that. However, the code below
  // should be practically unreachable.
  auto guard = detail::scope_guard{[&] noexcept {
    TENZIR_ERROR("aborting because async scope join failed");
    std::terminate();
  }};
  auto result = AsyncResult{
    co_await folly::coro::co_awaitTry(std::invoke(std::forward<F>(f), spawn))};
  // We only cancel the jobs if the given function failed or was cancelled.
  if (not result.is_value()) {
    TENZIR_DEBUG("cancelling async scope because of exception/cancellation");
    scope.requestCancellation();
  }
  // Provide a custom cancellation token to ensure that cancellation doesn't
  // abort the join. Because we did not ask the scope itself to store and throw
  // exceptions, this should always succeed (if it returns at all).
  auto join_result = AsyncResult{co_await folly::coro::co_awaitTry(
    folly::coro::co_withCancellation({}, scope.joinAsync()))};
  // Just in case, we still check explicitly.
  if (join_result.is_value()) {
    guard.disable();
  } else {
    guard.trigger();
  }
  // Now return the result of the user-provided function.
  co_return std::move(result).unwrap();
}

} // namespace tenzir
