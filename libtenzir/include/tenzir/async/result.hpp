//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/logger.hpp"

#include <folly/OperationCancelled.h>
#include <folly/Try.h>

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
  auto unwrap(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT_ALWAYS(not self.value_.hasException()
                           or self.value_.exception().has_exception_ptr(),
                         "AsyncResult::unwrap() called with empty exception "
                         "wrapper");
    return std::forward<Self>(self).value_.value();
  }

  template <class Self>
  auto exception(this Self&& self) -> decltype(auto) {
    TENZIR_ASSERT(not self.is_cancelled());
    return std::forward<Self>(self).value_.exception();
  }

  template <class Self>
  auto exception_or_cancelled(this Self&& self) -> decltype(auto) {
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

} // namespace tenzir
