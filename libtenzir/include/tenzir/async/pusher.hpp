//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/push_pull.hpp"
#include "tenzir/box.hpp"
#include "tenzir/pipeline_metrics.hpp"
#include "tenzir/series_builder.hpp"

#include <folly/coro/BoundedQueue.h>

#include <chrono>
#include <functional>

namespace tenzir {

/// Pushes results from series_builder and coordinates builder timeouts between
/// `Operator::process_task()` and `Operator::await_task()`.
///
/// The timeout store is a single-slot mailbox with latest-wins semantics:
/// pushing a result replaces a pending timeout instead of taking the minimum.
/// This is only correct because every push must carry the minimum remaining
/// wait across all deadlines the operator tracks, which
/// `series_builder::YieldReadyResult::merge()` guarantees by merging deadlines
/// by minimum.
///
/// Once `wait()` has dequeued a timeout, it is committed to sleeping for that
/// duration: a shorter deadline pushed during the sleep does not preempt it,
/// and a later one makes the wakeup stale. Operators therefore must not rely
/// on wakeups arriving on time and instead re-check their deadlines in
/// `process_task()`, rescheduling any remaining wait.
class SeriesPusher {
public:
  using duration = std::chrono::steady_clock::duration;

  SeriesPusher() : wait_for_{std::in_place, 1u} {
  }

  /// Waits for the next scheduled timeout and sleeps for that duration.
  auto wait() const -> Task<void> {
    auto next = co_await wait_for_->dequeue();
    co_await sleep_for(next);
  }

  /// Pushes one ready slice and schedules the next timeout.
  auto push(series_builder::YieldReadyResult result,
            Push<table_slice>& push) const -> Task<void> {
    co_await this->push(std::move(result), push, [](table_slice const&) {});
  }

  auto push(series_builder::YieldReadyResult result, Push<table_slice>& push,
            MetricsCounter& counter) const -> Task<void> {
    co_await this->push(std::move(result), push, [&](table_slice const& slice) {
      counter.add(slice.rows());
    });
  }

  template <class OnSlice>
  auto push(series_builder::YieldReadyResult result, Push<table_slice>& push,
            OnSlice&& on_slice) const -> Task<void> {
    for (auto&& slice : result.slices) {
      std::invoke(on_slice, slice);
      co_await push(std::move(slice));
    }
    if (result.wait_for) {
      set_wait_for(result.wait_for.unwrap());
    }
  }

private:
  auto set_wait_for(duration wait_for) const -> void {
    // Drop stale duration (if any) so consumers always wake with the freshest
    // remaining wait.
    wait_for_->try_dequeue();
    wait_for_->try_enqueue(wait_for);
  }

  mutable Box<folly::coro::BoundedQueue<duration>> wait_for_;
};

/// Tracks the flush deadline of a partially filled batch and coordinates the
/// wakeup between `Operator::await_task()` and `Operator::process_task()`.
///
/// Unlike `SeriesPusher`, this class is independent of the builder type: the
/// operator reports how many rows are pending via `poll()` and is told whether
/// the batch timeout has expired. The wakeup mailbox follows the same
/// latest-wins contract as `SeriesPusher`, so a wakeup may be stale and callers
/// must re-run `poll()` in `process_task()` instead of flushing unconditionally.
class BatchTimeout {
public:
  using clock = std::chrono::steady_clock;
  using duration = clock::duration;

  explicit BatchTimeout(duration timeout)
    : timeout_{timeout}, wait_for_{std::in_place, 1u} {
  }

  /// Sleeps until the currently scheduled deadline. Intended for `await_task()`.
  auto wait() const -> Task<void> {
    auto next = co_await wait_for_->dequeue();
    co_await sleep_for(next);
  }

  /// Reports the number of currently pending rows. Returns true if the caller
  /// must flush now because the deadline of the oldest pending row has expired.
  /// Otherwise (re)schedules the wakeup for the remaining wait.
  auto poll(size_t rows, clock::time_point now = clock::now()) -> bool {
    if (rows == 0) {
      oldest_ = None{};
      return false;
    }
    if (not oldest_) {
      oldest_ = now;
      schedule(timeout_);
      return false;
    }
    auto const waiting = now - *oldest_;
    if (waiting >= timeout_) {
      oldest_ = None{};
      return true;
    }
    schedule(timeout_ - waiting);
    return false;
  }

  /// Forgets the current deadline, e.g., after a size-triggered flush.
  auto reset() -> void {
    oldest_ = None{};
  }

private:
  auto schedule(duration wait_for) const -> void {
    // Drop stale duration (if any) so consumers always wake with the freshest
    // remaining wait.
    wait_for_->try_dequeue();
    wait_for_->try_enqueue(wait_for);
  }

  duration timeout_;
  Option<clock::time_point> oldest_;
  mutable Box<folly::coro::BoundedQueue<duration>> wait_for_;
};

} // namespace tenzir
