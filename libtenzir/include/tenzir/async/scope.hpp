//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/log.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/async/result.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/unit.hpp"

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
  ///
  /// This will panic if the underlying task was cancelled.
  auto join() -> Task<T> {
    // TODO: Make this thread-safe.
    TENZIR_ASSERT(state_);
    TENZIR_ASSERT(not state_->joined);
    co_await state_->notify.wait();
    TENZIR_ASSERT(not state_->joined);
    state_->joined = true;
    if (not state_->value) {
      panic("joining a task that got cancelled");
    }
    if constexpr (not std::is_void_v<T>) {
      co_return std::move(*state_->value);
    }
  }

private:
  friend class AsyncScope;

  struct State {
    bool joined = false;
    Notify notify;
    std::optional<VoidToUnit<T>> value;
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
    LOGV("spawning task in scope {}", fmt::ptr(this));
    using Result = folly::coro::semi_await_result_t<SemiAwaitable>;
    auto state = std::make_shared<typename AsyncHandle<Result>::State>();
    scope_.add(
      folly::coro::co_withExecutor(
        executor_,
        folly::coro::co_invoke([this, state,
                                awaitable = std::forward<SemiAwaitable>(
                                  awaitable)] mutable -> Task<void> {
          auto result = AsyncResult{co_await folly::coro::co_awaitTry(
            folly::coro::co_withCancellation(cancel_token_,
                                             std::move(awaitable)))};
          if (result.is_exception()) {
            // TODO: This needs to be made thread-safe!
            if (not exception_) {
              LOGE("remembering exception for scope {}: {}", fmt::ptr(this),
                   result.exception().what());
              exception_ = std::move(result).exception();
              cancel_source_.requestCancellation();
            } else {
              LOGE("dropping exception for scope {}: {}", fmt::ptr(this),
                   result.exception().what());
            }
            co_return;
          }
          if (result.is_value()) {
            if constexpr (std::is_void_v<Result>) {
              state->value = Unit{};
            } else {
              state->value = std::move(result).unwrap();
            }
          } else {
            LOGV("task of scope {} got cancelled", fmt::ptr(this));
          }
          state->notify.notify_one();
        })),
      FOLLY_ASYNC_STACK_RETURN_ADDRESS());
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
    // TODO: Does this also cancel the main task of the scope?
    cancel_source_.requestCancellation();
  }

  auto is_cancelled() const -> bool {
    return cancel_token_.isCancellationRequested();
  }

private:
  AsyncScope(const AsyncScope&) = delete;
  auto operator=(const AsyncScope&) -> AsyncScope& = delete;
  AsyncScope(AsyncScope&&) = delete;
  auto operator=(AsyncScope&&) -> AsyncScope& = delete;

  AsyncScope(folly::ExecutorKeepAlive<> executor,
             folly::coro::AsyncScope& scope,
             folly::CancellationSource& cancel_source,
             folly::CancellationToken& cancel_token,
             folly::exception_wrapper& exception)
    : executor_{std::move(executor)},
      scope_{scope},
      cancel_source_{cancel_source},
      cancel_token_{cancel_token},
      exception_{exception} {
  }
  ~AsyncScope() = default;

  folly::ExecutorKeepAlive<> executor_;
  folly::coro::AsyncScope& scope_;
  folly::CancellationSource& cancel_source_;
  folly::CancellationToken& cancel_token_;
  folly::exception_wrapper& exception_;

  template <class F>
    requires std::invocable<F, AsyncScope&>
  friend auto async_scope(F&& f) -> Task<
    folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>>;
};

/// Provides a scope that can spawn tasks for structured concurrency.
///
/// The given function and all tasks spawned may access external objects as long
/// as they outlive this function call. It will only return once all spawned
/// tasks have terminated.
///
/// If the function is cancelled, then all spawned tasks will be cancelled as
/// well and cancellation will be propagated. If just a spawned task is
/// cancelled, execution will continue normally.
///
/// If the function or a spawned tasks fails with an exception, then everything
/// is cancelled and the exception is propagated.
///
/// Fun fact: This function is one of the very few things that would not be
/// possible in Rust, since implementing it requires async cancellation.
template <class F>
  requires std::invocable<F, AsyncScope&>
auto async_scope(F&& f) -> Task<
  folly::coro::semi_await_result_t<std::invoke_result_t<F, AsyncScope&>>> {
  auto exception = folly::exception_wrapper{};
  auto cancel_source = folly::CancellationSource{};
  auto cancel_token = folly::cancellation_token_merge(
    co_await folly::coro::co_current_cancellation_token,
    cancel_source.getToken());
  auto scope = folly::coro::AsyncScope{};
  auto wrapper = AsyncScope{
    co_await folly::coro::co_current_executor,
    scope,
    cancel_source,
    cancel_token,
    exception,
  };
  LOGV("created scope {}", fmt::ptr(&wrapper));
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
    co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
      cancel_token, std::invoke(std::forward<F>(f), wrapper)))};
  // We only cancel the jobs if the given function failed or was cancelled.
  if (not result.is_value()) {
    if (not cancel_token.isCancellationRequested()) {
      LOGV("cancelling async scope because of exception/cancellation");
      cancel_source.requestCancellation();
    }
  }
  // Provide a custom cancellation token to ensure that cancellation doesn't
  // abort the join. Because we did not ask the scope itself to store and throw
  // exceptions, this should always succeed (if it returns at all).
  LOGV("joining scope {} (cancelled = {})", fmt::ptr(&wrapper),
       cancel_token.isCancellationRequested());
  auto join_result = AsyncResult{co_await folly::coro::co_awaitTry(
    folly::coro::co_withCancellation({}, scope.joinAsync()))};
  LOGV("joined scope {}", fmt::ptr(&wrapper));
  // Just in case, we still check explicitly.
  if (join_result.is_value()) {
    guard.disable();
  } else {
    guard.trigger();
  }
  // If any of the spawned tasks threw, we need to propagate the exception.
  if (exception) {
    LOGE("leaving scope {} with exception: {}", fmt::ptr(&wrapper),
         exception.what());
    exception.throw_exception();
  }
  LOGV("leaving scope {}", fmt::ptr(&wrapper));
  // Now return the result of the user-provided function.
  co_return std::move(result).unwrap();
}

} // namespace tenzir
