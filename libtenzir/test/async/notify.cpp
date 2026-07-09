//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/notify.hpp"

#include <folly/OperationCancelled.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace tenzir {

TEST("notify before wait completes immediately") {
  auto notify = Notify{};
  notify.notify_one();
  folly::coro::blockingWait(notify.wait());
}

TEST("notifications do not stack") {
  auto notify = Notify{};
  notify.notify_one();
  notify.notify_one();
  folly::coro::blockingWait([&]() -> Task<void> {
    co_await notify.wait();
    // The second notification was coalesced into the first, so another wait
    // must block until a new notification arrives.
    auto second_done = false;
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await notify.wait();
        second_done = true;
      }(),
      [&]() -> Task<void> {
        check(not second_done);
        notify.notify_one();
        co_return;
      }());
    check(second_done);
  }());
}

TEST("notify wakes exactly one of two waiters") {
  auto notify = Notify{};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto done = std::array{false, false};
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await notify.wait();
        done[0] = true;
      }(),
      [&]() -> Task<void> {
        co_await notify.wait();
        done[1] = true;
      }(),
      [&]() -> Task<void> {
        notify.notify_one();
        co_await folly::coro::co_reschedule_on_current_executor;
        // Only one waiter may have consumed the notification.
        check_ne(done[0], done[1]);
        notify.notify_one();
      }());
    check(done[0]);
    check(done[1]);
  }());
}

TEST("wait with cancelled token does not consume a notification") {
  auto notify = Notify{};
  notify.notify_one();
  auto cancel = folly::CancellationSource{};
  cancel.requestCancellation();
  auto cancelled = false;
  folly::coro::blockingWait([&]() -> Task<void> {
    try {
      co_await folly::coro::co_withCancellation(cancel.getToken(),
                                                notify.wait());
    } catch (folly::OperationCancelled const&) {
      cancelled = true;
    }
  }());
  check(cancelled);
  // The notification must still be there.
  folly::coro::blockingWait(notify.wait());
}

TEST("cancelled waiter hands its notification to the next waiter") {
  auto notify = Notify{};
  auto cancel = folly::CancellationSource{};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto first_cancelled = false;
    auto second_done = false;
    co_await folly::coro::collectAll(
      // First waiter: suspends first (FIFO), so the notification gets
      // assigned to it, but it observes cancellation on resumption.
      [&]() -> Task<void> {
        try {
          co_await folly::coro::co_withCancellation(cancel.getToken(),
                                                    notify.wait());
        } catch (folly::OperationCancelled const&) {
          first_cancelled = true;
        }
      }(),
      [&]() -> Task<void> {
        co_await notify.wait();
        second_done = true;
      }(),
      [&]() -> Task<void> {
        // Assign the notification to the first waiter and cancel it before
        // its continuation runs. It must hand the notification over instead
        // of dropping or consuming it.
        notify.notify_one();
        cancel.requestCancellation();
        co_return;
      }());
    check(first_cancelled);
    check(second_done);
  }());
}

TEST("cancelled waiter with no successor stores the notification") {
  auto notify = Notify{};
  auto cancel = folly::CancellationSource{};
  auto cancelled = false;
  folly::coro::blockingWait([&]() -> Task<void> {
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        try {
          co_await folly::coro::co_withCancellation(cancel.getToken(),
                                                    notify.wait());
        } catch (folly::OperationCancelled const&) {
          cancelled = true;
        }
      }(),
      [&]() -> Task<void> {
        notify.notify_one();
        cancel.requestCancellation();
        co_return;
      }());
  }());
  check(cancelled);
  // The notification survived the cancelled waiter.
  folly::coro::blockingWait(notify.wait());
}

TEST("cancellation without notification wakes only the cancelled waiter") {
  auto notify = Notify{};
  auto cancel = folly::CancellationSource{};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto first_cancelled = false;
    auto second_done = false;
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        try {
          co_await folly::coro::co_withCancellation(cancel.getToken(),
                                                    notify.wait());
        } catch (folly::OperationCancelled const&) {
          first_cancelled = true;
        }
      }(),
      [&]() -> Task<void> {
        co_await notify.wait();
        second_done = true;
      }(),
      [&]() -> Task<void> {
        cancel.requestCancellation();
        co_await folly::coro::co_reschedule_on_current_executor;
        check(first_cancelled);
        // The second waiter must not have been woken spuriously.
        check(not second_done);
        notify.notify_one();
      }());
    check(second_done);
  }());
}

TEST("wait can be cancelled from another thread") {
  auto notify = Notify{};
  auto cancel = folly::CancellationSource{};
  auto started = std::promise<void>{};
  auto started_future = started.get_future();
  auto cancelled = std::atomic<bool>{false};
  auto worker = std::thread{[&] {
    try {
      folly::coro::blockingWait(folly::coro::co_withCancellation(
        cancel.getToken(), [&]() -> Task<void> {
          started.set_value();
          co_await notify.wait();
        }()));
    } catch (folly::OperationCancelled const&) {
      cancelled.store(true, std::memory_order_release);
    }
  }};
  check_eq(started_future.wait_for(std::chrono::seconds{1}),
           std::future_status::ready);
  cancel.requestCancellation();
  worker.join();
  check(cancelled.load(std::memory_order_acquire));
}

TEST("concurrent notifications are neither lost nor duplicated") {
  auto notify = Notify{};
  constexpr auto num_notifications = 10'000;
  auto consumed = std::atomic<int>{0};
  auto consumer = std::thread{[&] {
    folly::coro::blockingWait([&]() -> Task<void> {
      for (auto i = 0; i < num_notifications; ++i) {
        co_await notify.wait();
        consumed.fetch_add(1, std::memory_order_relaxed);
      }
    }());
  }};
  auto producer = std::thread{[&] {
    for (auto i = 0; i < num_notifications; ++i) {
      // Wait until the previous notification was consumed so that none of
      // them coalesce and the count stays exact.
      while (consumed.load(std::memory_order_relaxed) < i) {
        std::this_thread::yield();
      }
      notify.notify_one();
    }
  }};
  producer.join();
  consumer.join();
  check_eq(consumed.load(std::memory_order_relaxed), num_notifications);
}

} // namespace tenzir
