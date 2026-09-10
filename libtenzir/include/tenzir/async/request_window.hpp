//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/bounded_queue.hpp"
#include "tenzir/async/task.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

namespace tenzir {

/// Bounds the number of requests a sink keeps in flight at the same time.
///
/// A sink that sends one request at a time spends most of its time waiting for
/// responses. This window lets it start up to `capacity` requests before it
/// has to wait for the oldest one, without introducing any concurrent access
/// to operator state: the spawned tasks only run the request itself and hand
/// the result back through a queue. Every completion is observed by the
/// operator driver in `acquire()` or `drain()`, so diagnostics and metrics are
/// still emitted from the operator's own execution context.
///
/// Requests may complete in any order, and `capacity > 1` therefore gives up
/// the guarantee that the target observes requests in the order the sink
/// produced them. Only use a window larger than one where that is acceptable.
///
/// Call `drain()` before finishing a checkpoint or finalizing, so that no
/// request outlives the operator. Waiting for the next `acquire()` or `drain()`
/// alone can delay a completion indefinitely when no further input arrives, so
/// `spawn()` takes a wakeup callback: use it to wake the operator driver, and
/// call `poll()` from `process_task()` so that every completion is handled
/// promptly.
template <class Completion>
class RequestWindow {
public:
  /// Capacities are clamped to what the completion queue can represent, so
  /// that no user-supplied value can panic the constructor. Beyond that bound
  /// the window is not a meaningful limit anyway.
  explicit RequestWindow(size_t capacity)
    : capacity_{std::clamp(capacity, size_t{1},
                           size_t{std::numeric_limits<uint32_t>::max()})},
      completions_{std::in_place, static_cast<uint32_t>(capacity_)} {
  }

  auto capacity() const -> size_t {
    return capacity_;
  }

  auto in_flight() const -> size_t {
    return in_flight_;
  }

  /// Starts a request in a free slot. The caller must have made sure that a
  /// slot is free by awaiting `acquire()` first.
  ///
  /// `make_request` must return a `Task<Completion>` and must not touch
  /// operator state; it runs concurrently with the operator.
  ///
  /// `wakeup` runs after the completion is enqueued. It must not touch
  /// operator state and must not block; use it to wake the operator driver so
  /// that `poll()` handles the completion even when no further input arrives.
  /// It must own whatever it signals, because it may run after the operator
  /// drained the completion and moved on.
  template <class F, class W>
  auto spawn(OpCtx& ctx, F make_request, W wakeup) -> void {
    TENZIR_ASSERT(in_flight_ < capacity_);
    ++in_flight_;
    // The queue has room for every in-flight request, so the enqueue below
    // never blocks and a finished request always releases its slot.
    std::ignore = ctx.spawn_task(
      [completions = completions_, make_request = std::move(make_request),
       wakeup = std::move(wakeup)]() mutable -> Task<void> {
        co_await completions->enqueue(co_await make_request());
        wakeup();
      });
  }

  /// Passes every completion that has already arrived to `handle`, without
  /// waiting. Call this when the wakeup passed to `spawn()` fires.
  template <class F>
  auto poll(F&& handle) -> void {
    while (in_flight_ > 0) {
      auto completion = completions_->try_dequeue();
      if (completion.is_none()) {
        return;
      }
      --in_flight_;
      handle(std::move(*completion));
    }
  }

  /// Waits until at least one slot is free, passing every completed request to
  /// `handle`.
  template <class F>
  auto acquire(F&& handle) -> Task<void> {
    // Handle finished requests eagerly so that their outcome does not wait
    // for the window to fill up.
    poll(handle);
    while (in_flight_ >= capacity_) {
      co_await take_one(handle);
    }
  }

  /// Waits for all in-flight requests, passing each one to `handle`.
  template <class F>
  auto drain(F&& handle) -> Task<void> {
    while (in_flight_ > 0) {
      co_await take_one(handle);
    }
  }

private:
  template <class F>
  auto take_one(F& handle) -> Task<void> {
    TENZIR_ASSERT(in_flight_ > 0);
    auto completion = co_await completions_->dequeue();
    --in_flight_;
    handle(std::move(completion));
  }

  size_t capacity_;
  size_t in_flight_ = 0;
  /// Results of the spawned request tasks. Producers are the request tasks,
  /// the only consumer is the operator driver.
  Arc<BoundedQueue<Completion, false, true>> completions_;
};

} // namespace tenzir
