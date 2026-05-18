//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/bounded_queue.hpp"

#include "tenzir/async/task.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <atomic>
#include <chrono>

namespace tenzir {

namespace {

// A move-only, non-default-constructible value type to ensure the queue does
// not depend on `T` being default-constructible or copyable.
struct MoveOnly {
  explicit MoveOnly(int x) : value{x} {
  }

  ~MoveOnly() = default;
  MoveOnly(const MoveOnly&) = delete;
  auto operator=(const MoveOnly&) -> MoveOnly& = delete;
  MoveOnly(MoveOnly&&) noexcept = default;
  auto operator=(MoveOnly&&) noexcept -> MoveOnly& = default;

  int value;
};

} // namespace

TEST("bounded_queue dequeues regular enqueues in FIFO order") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{4};
    co_await queue.enqueue(1);
    co_await queue.enqueue(2);
    co_await queue.enqueue(3);
    check_eq(queue.size(), size_t{3});
    check_eq(co_await queue.dequeue(), 1);
    check_eq(co_await queue.dequeue(), 2);
    check_eq(co_await queue.dequeue(), 3);
    check(queue.empty());
  }());
}

TEST("bounded_queue force_enqueue exceeds capacity without blocking") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{1};
    co_await queue.enqueue(1);
    // Fills the queue beyond `capacity` synchronously — must not suspend.
    queue.force_enqueue(2);
    queue.force_enqueue(3);
    check_eq(queue.size(), size_t{3});
    // FIFO is preserved: regular item enqueued first comes out first.
    check_eq(co_await queue.dequeue(), 1);
    check_eq(co_await queue.dequeue(), 2);
    check_eq(co_await queue.dequeue(), 3);
    check(queue.empty());
  }());
}

TEST("bounded_queue forced dequeue does not release a capacity slot") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{1};
    // `force_enqueue` first, while a slot is still available — the slot is
    // NOT consumed by a forced item.
    queue.force_enqueue(10);
    // A regular enqueue still takes the only slot.
    co_await queue.enqueue(20);
    check_eq(queue.size(), size_t{2});
    // No slot free, since the regular item is in flight.
    check_eq(queue.try_enqueue(30), false);
    // Dequeue the forced item first (FIFO). Capacity stays consumed.
    check_eq(co_await queue.dequeue(), 10);
    check_eq(queue.try_enqueue(30), false);
    // Dequeue the regular item — now the slot is released.
    check_eq(co_await queue.dequeue(), 20);
    check_eq(queue.try_enqueue(30), true);
    check_eq(co_await queue.dequeue(), 30);
  }());
}

TEST("bounded_queue try_enqueue fails at capacity but force_enqueue still "
     "succeeds") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{2};
    check_eq(queue.try_enqueue(1), true);
    check_eq(queue.try_enqueue(2), true);
    check_eq(queue.try_enqueue(3), false);
    queue.force_enqueue(99);
    check_eq(queue.size(), size_t{3});
    check_eq(co_await queue.dequeue(), 1);
    check_eq(co_await queue.dequeue(), 2);
    check_eq(co_await queue.dequeue(), 99);
  }());
}

TEST("bounded_queue try_dequeue returns None on empty") {
  auto queue = BoundedQueue<int>{1};
  auto first = queue.try_dequeue();
  check(first.is_none());
  check(queue.try_enqueue(7));
  auto second = queue.try_dequeue();
  check(second.is_some());
  check_eq(*second, 7);
  auto third = queue.try_dequeue();
  check(third.is_none());
}

TEST("bounded_queue enqueue blocks at capacity and resumes on dequeue") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{1};
    co_await queue.enqueue(1);
    auto producer_finished = std::atomic<bool>{false};
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await queue.enqueue(2);
        producer_finished.store(true, std::memory_order_release);
      }(),
      [&]() -> Task<void> {
        // Yield a few times to give the producer a chance to start.
        for (auto i = 0; i < 8; ++i) {
          co_await folly::coro::co_reschedule_on_current_executor;
        }
        check_eq(producer_finished.load(std::memory_order_acquire), false);
        check_eq(co_await queue.dequeue(), 1);
      }());
    check(producer_finished.load(std::memory_order_acquire));
    check_eq(co_await queue.dequeue(), 2);
  }());
}

TEST("bounded_queue forced dequeue does not unblock a waiting enqueue") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{1};
    co_await queue.enqueue(1); // Consumes the only slot.
    queue.force_enqueue(2);    // Bypasses the gate.
    auto producer_finished = std::atomic<bool>{false};
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await queue.enqueue(3); // Must suspend; slot held by `1`.
        producer_finished.store(true, std::memory_order_release);
      }(),
      [&]() -> Task<void> {
        for (auto i = 0; i < 8; ++i) {
          co_await folly::coro::co_reschedule_on_current_executor;
        }
        // Drain the forced item first. The waiter must remain suspended
        // because forced dequeues never release the gate.
        check_eq(co_await queue.dequeue(), 1);
        // After the regular item drains, the slot frees up and the waiter
        // resumes — but only after we yield enough times for it to run.
        for (auto i = 0; i < 8; ++i) {
          co_await folly::coro::co_reschedule_on_current_executor;
        }
        check(producer_finished.load(std::memory_order_acquire));
      }());
    // Order seen by the consumer: 1 (regular), 2 (forced), 3 (regular).
    check_eq(co_await queue.dequeue(), 2);
    check_eq(co_await queue.dequeue(), 3);
  }());
}

TEST("bounded_queue try_dequeue_for times out on empty queue") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<int>{1};
    auto result = co_await folly::coro::co_awaitTry(
      queue.try_dequeue_for(std::chrono::milliseconds{10}));
    check(result.hasException());
  }());
}

TEST("bounded_queue works with move-only non-default-constructible types") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto queue = BoundedQueue<MoveOnly>{2};
    co_await queue.enqueue(MoveOnly{1});
    queue.force_enqueue(MoveOnly{2});
    auto a = co_await queue.dequeue();
    auto b = co_await queue.dequeue();
    check_eq(a.value, 1);
    check_eq(b.value, 2);
  }());
}

} // namespace tenzir
