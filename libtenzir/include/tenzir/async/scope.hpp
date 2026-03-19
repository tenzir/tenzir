//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
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

#include <mutex>

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
    data_.scope.add(
      folly::coro::co_withExecutor(
        data_.executor,
        folly::coro::co_invoke([this, state,
                                awaitable = std::forward<SemiAwaitable>(
                                  awaitable)] mutable -> Task<void> {
          auto result = AsyncResult{co_await folly::coro::co_awaitTry(
            folly::coro::co_withCancellation(data_.cancel_token,
                                             std::move(awaitable)))};
          if (result.is_exception()) {
            auto lock = std::scoped_lock{data_.exception_mutex};
            if (not data_.exception) {
              LOGE("remembering exception for scope {}: {}", fmt::ptr(this),
                   result.exception().what());
              data_.exception = std::move(result).exception();
              data_.cancel_source.requestCancellation();
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
  auto cancel() -> void {
    data_.cancel_source.requestCancellation();
  }

  auto is_cancelled() const -> bool {
    return data_.cancel_token.isCancellationRequested();
  }

private:
  struct Data {
    folly::ExecutorKeepAlive<> executor;
    folly::coro::AsyncScope scope;
    folly::CancellationSource cancel_source;
    folly::CancellationToken cancel_token;
    folly::exception_wrapper exception;
    std::mutex exception_mutex;
  };

  AsyncScope(const AsyncScope&) = delete;
  auto operator=(const AsyncScope&) -> AsyncScope& = delete;
  AsyncScope(AsyncScope&&) = delete;
  auto operator=(AsyncScope&&) -> AsyncScope& = delete;

  explicit AsyncScope(Data& data) : data_{data} {
  }
  ~AsyncScope() = default;

  Data& data_;

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
  auto data = AsyncScope::Data{};
  data.cancel_token = folly::cancellation_token_merge(
    co_await folly::coro::co_current_cancellation_token,
    data.cancel_source.getToken());
  data.executor = co_await folly::coro::co_current_executor;
  auto scope = AsyncScope{data};
  LOGV("created scope {}", fmt::ptr(&scope));
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
  auto result = AsyncResult{co_await folly::coro::co_awaitTry(
    folly::coro::co_withCancellation(data.cancel_token,
                                     std::invoke(std::forward<F>(f), scope)))};
  // We only cancel the jobs if the given function failed or was cancelled.
  if (not result.is_value()) {
    if (not data.cancel_token.isCancellationRequested()) {
      LOGV("cancelling async scope because of exception/cancellation");
      data.cancel_source.requestCancellation();
    }
  }
  // Provide a custom cancellation token to ensure that cancellation doesn't
  // abort the join. Because we did not ask the scope itself to store and throw
  // exceptions, this should always succeed (if it returns at all).
  LOGV("joining scope {} (cancelled = {})", fmt::ptr(&scope),
       data.cancel_token.isCancellationRequested());
  auto join_result = AsyncResult{co_await folly::coro::co_awaitTry(
    folly::coro::co_withCancellation({}, data.scope.joinAsync()))};
  LOGV("joined scope {}", fmt::ptr(&scope));
  // Just in case, we still check explicitly.
  if (join_result.is_value()) {
    guard.disable();
  } else {
    guard.trigger();
  }
  // If any of the spawned tasks threw, we need to propagate the exception. No
  // need to lock it, as the tasks are already joined.
  if (data.exception) {
    LOGE("leaving scope {} with exception: {}", fmt::ptr(&scope),
         data.exception.what());
    data.exception.throw_exception();
  }
  LOGV("leaving scope {}", fmt::ptr(&scope));
  // Now return the result of the user-provided function.
  co_return std::move(result).unwrap();
}

} // namespace tenzir
