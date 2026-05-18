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

#include <functional>

namespace tenzir {

/// Pushes results from series_builder and coordinates builder timeouts between
/// `Operator::process_task()` and `Operator::await_task()`.
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

} // namespace tenzir
