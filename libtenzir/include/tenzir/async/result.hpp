//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/result.hpp"

#include <tenzir/async/task.hpp>
#include <tenzir/type_traits.hpp>

namespace tenzir {

/// The reified result of an cancellable computation.
///
/// Either it produced a value, or it failed, or it was cancelled.
template <class T>
class AsyncResult {
public:
  AsyncResult() = default;
  ~AsyncResult() = default;

  AsyncResult(AsyncResult<T>&& other) = default;
  auto operator=(AsyncResult<T>&&) -> AsyncResult<T>& = default;
  AsyncResult(const AsyncResult<T>& other) = default;
  auto operator=(const AsyncResult<T>&) -> AsyncResult<T>& = default;

  template <class U>
    requires std::convertible_to<U, T>
  explicit(false) AsyncResult(AsyncResult<U> other)
    : value_{other.is_value() ? folly::Try<T>{std::move(other).value()}
                              : folly::Try<T>{std::move(other).exception()}} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) AsyncResult(U&& value) : value_{std::forward<U>(value)} {
  }

  explicit(false) AsyncResult(folly::exception_wrapper ew)
    : value_{std::move(ew)} {
  }

  explicit(false) AsyncResult(folly::Try<T> value) : value_{std::move(value)} {
  }

  template <class U>
    requires(std::convertible_to<U, T>)
  explicit(false) AsyncResult(folly::Try<U> value) {
    if (value.hasValue()) {
      value_.emplace(std::move(value).value());
    } else {
      value_.emplaceException(std::move(value).exception());
    }
  }

  template <class Self>
  auto value(this Self&& self) -> decltype(auto) {
    return std::forward<Self>(self).value_.value();
  }

  template <class Self>
  auto exception(this Self&& self)
    -> ForwardLike<Self, folly::exception_wrapper> {
    TENZIR_ASSERT(not self.is_cancelled());
    return std::forward<Self>(self).value_.exception();
  }

  template <class Self>
  auto exception_or_cancelled(this Self&& self)
    -> ForwardLike<Self, folly::exception_wrapper> {
    return std::forward<Self>(self).value_.exception();
  }

  auto is_cancelled() const -> bool {
    return value_.template hasException<folly::OperationCancelled>();
  }

  auto is_value() const -> bool {
    return value_.hasValue();
  }

  auto is_exception() const -> bool {
    return value_.hasException() and not is_cancelled();
  }

private:
  // Based on `Try` because `result` is not default-constructible.
  folly::Try<T> value_;
};

/// Wraps an awaitable to catch exceptions and cancellations.
///
/// Always prefer this over `folly::coro::co_awaitTry`. Unlike `coro::Try<T>`,
/// we separate cancellation from exceptions. See https://wg21.link/p1677 for
/// why this is a good idea.
///
/// Only use this if you want to special-case cancellation. If you don't care
/// about it, please use `async_try` instead.
template <class SemiAwaitable>
auto async_result(SemiAwaitable&& awaitable)
  -> Task<AsyncResult<folly::coro::semi_await_result_t<SemiAwaitable>>> {
  // Note: This could be optimized to avoid the additional coroutine frame.
  co_return AsyncResult{
    co_await folly::coro::co_awaitTry(std::forward<SemiAwaitable>(awaitable))};
}

/// Wraps an awaitable to catch exceptions, while propagating cancellation.
///
/// Always prefer `folly::coro::co_awaitTry`. If you want to special-case
/// cancellation, please use `async_result` instead.
template <class SemiAwaitable>
auto async_try(SemiAwaitable&& awaitable)
  -> Task<Result<folly::coro::semi_await_result_t<SemiAwaitable>,
                 folly::exception_wrapper>> {
  // Note: This could be optimized to avoid the additional coroutine frame.
  auto result = co_await async_result(std::forward<SemiAwaitable>(awaitable));
  if (result.is_exception()) {
    co_return Err{std::move(result).exception()};
  }
  // This call will propagate cancellation.
  co_return std::move(result).value();
}

} // namespace tenzir
