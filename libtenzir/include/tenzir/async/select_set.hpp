//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/join_set.hpp"
#include "tenzir/panic.hpp"

#include <vector>

namespace tenzir {

/// A `JoinSet` that the consumer can select/filter over.
///
/// The `next` function of this class accepts a filter that decides which
/// results should be returned and what should remain buffered in case it is
/// already done. This is useful when a caller multiplexes several kinds of
/// events through one completion stream but only wants to act on a subset at
/// any given point.
///
/// This type has the same safety requirements as `JoinSet`.
template <class T>
class SelectSet {
public:
  /// See `JoinSet::activate()`.
  template <class F>
  auto activate(F&& f)
    -> Task<folly::coro::semi_await_result_t<std::invoke_result_t<F>>> {
    return set_.activate(std::forward<F>(f));
  }

  /// Wait for the next item that satisfies the filter.
  ///
  /// Existing buffered items are checked first before waiting for newly
  /// completed tasks. If the underlying task set finishes while unmatched items
  /// remain buffered, this function panics because the caller made no progress
  /// on the remaining event classes.
  template <class Filter>
    requires(std::is_invocable_r_v<bool, Filter, T const&>)
  auto next(Filter&& filter) -> Task<Option<T>> {
    // First, inspect the existing items to see if one qualifies.
    for (auto i = size_t{0}; i < existing_.size(); ++i) {
      if (std::invoke(filter, std::as_const(existing_[i]))) {
        auto value = std::move(existing_[i]);
        existing_.erase(existing_.begin() + static_cast<std::ptrdiff_t>(i));
        co_return value;
      }
    }
    // If nothing was found, wait for new items to arrive.
    while (auto value = co_await set_.next()) {
      if (std::invoke(filter, std::as_const(*value))) {
        co_return value;
      }
      existing_.push_back(std::move(*value));
    }
    // All tasks are done. If some of it is unprocessed by now, we would deadlock.
    if (not existing_.empty()) {
      panic("got {} outstanding items without progress", existing_.size());
    }
    co_return None{};
  }

  /// See `JoinSet::add()`.
  template <class U>
    requires std::convertible_to<U, T>
  auto add(Task<U> task) -> void {
    return set_.add(std::move(task));
  }

  /// See `JoinSet::add()`.
  template <class F>
    requires std::convertible_to<
      folly::coro::semi_await_result_t<std::invoke_result_t<F>>, T>
  auto add(F&& f) -> void {
    return set_.add(std::forward<F>(f));
  }

  /// See `JoinSet::running()`.
  auto running() const -> size_t {
    return set_.running();
  }

private:
  JoinSet<T> set_;
  std::vector<T> existing_;
};

} // namespace tenzir
