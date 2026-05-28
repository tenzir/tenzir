//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <folly/coro/Task.h>
#include <folly/coro/UnboundedQueue.h>
#include <folly/fibers/Semaphore.h>

#include <cstdint>
#include <utility>

namespace tenzir {

/// A bounded MPMC queue with an escape hatch for forced enqueues.
///
/// `enqueue` waits on a semaphore until a regular slot is free. `force_enqueue`
/// always succeeds immediately, even if the queue already holds `capacity`
/// (or more) items. A forced item, when later dequeued, does not free a
/// slot for waiting regular enqueuers: the capacity gate accounts only for
/// regular items, so `size()` may transiently exceed `capacity` until the
/// forced items drain.
///
/// FIFO ordering is preserved across the mix of regular and forced items
/// (by the time they hit the backing queue, as is standard for MPMC).
///
/// The `SingleProducer` and `SingleConsumer` template flags forward to the
/// backing `folly::coro::UnboundedQueue`. Crucially, they constrain *all*
/// producer- and consumer-side calls respectively: setting `SingleProducer`
/// means no two of `enqueue` / `try_enqueue` / `force_enqueue` may be in
/// flight concurrently, since `force_enqueue` is itself a producer against
/// the inner queue.
template <class T, bool SingleProducer = false, bool SingleConsumer = false>
class BoundedQueue {
public:
  explicit BoundedQueue(uint32_t capacity) : sem_{capacity} {
  }

  ~BoundedQueue() = default;
  BoundedQueue(const BoundedQueue&) = delete;
  auto operator=(const BoundedQueue&) -> BoundedQueue& = delete;
  BoundedQueue(BoundedQueue&&) = delete;
  auto operator=(BoundedQueue&&) -> BoundedQueue& = delete;

  /// Enqueue an item, waiting for a free slot if the queue is at capacity.
  template <class U = T>
  auto enqueue(U&& item) -> folly::coro::Task<void> {
    co_await folly::coro::co_nothrow(sem_.co_wait());
    queue_.enqueue(Slot{T(std::forward<U>(item)), /*forced=*/false});
  }

  /// Enqueue without blocking. Returns true on success, false if at capacity.
  template <class U = T>
  auto try_enqueue(U&& item) -> bool {
    if (not sem_.try_wait()) {
      return false;
    }
    queue_.enqueue(Slot{T(std::forward<U>(item)), /*forced=*/false});
    return true;
  }

  /// Enqueue an item immediately, exceeding capacity if necessary.
  ///
  /// The capacity gate is not consumed on enqueue and not released on the
  /// matching dequeue. Use for sentinels, shutdown markers, or other control
  /// values that must reach the consumer even when the queue is full.
  template <class U = T>
  auto force_enqueue(U&& item) -> void {
    queue_.enqueue(Slot{T(std::forward<U>(item)), /*forced=*/true});
  }

  /// Dequeue an item, waiting if the queue is empty.
  auto dequeue() -> folly::coro::Task<T> {
    auto slot = co_await queue_.dequeue();
    if (not slot.forced) {
      sem_.signal();
    }
    co_return std::move(slot.value);
  }

  /// Dequeue with a timeout. On timeout, the returned task completes with
  /// `folly::OperationCancelled`, matching the inner queue's semantics.
  template <class Duration>
  auto try_dequeue_for(Duration timeout) -> folly::coro::Task<T> {
    auto slot = co_await queue_.co_try_dequeue_for(timeout);
    if (not slot.forced) {
      sem_.signal();
    }
    co_return std::move(slot.value);
  }

  /// Dequeue without blocking. Returns `None` if the queue is empty.
  auto try_dequeue() -> Option<T> {
    auto slot = queue_.try_dequeue();
    if (not slot) {
      return None{};
    }
    if (not slot->forced) {
      sem_.signal();
    }
    return Option<T>{std::move(slot->value)};
  }

  auto empty() const -> bool {
    return queue_.empty();
  }

  auto size() const -> size_t {
    return queue_.size();
  }

private:
  // Items are tagged with whether they were forced so that the dequeue side
  // knows whether to release a capacity slot. We cannot derive this from
  // counters alone because FIFO ordering means a dequeued item could be
  // either a regular or a forced one — without the tag, signaling on every
  // dequeue would push the token count above `capacity` and break the gate.
  struct Slot {
    T value;
    bool forced;
  };

  folly::coro::UnboundedQueue<Slot, SingleProducer, SingleConsumer> queue_;
  folly::fibers::Semaphore sem_;
};

} // namespace tenzir
