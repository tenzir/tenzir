//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/channel.hpp"

#include "tenzir/async/blocking_executor.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <array>
#include <atomic>
#include <memory>
#include <set>
#include <thread>

namespace tenzir {

namespace {

struct NonDefaultMoveOnly {
  explicit NonDefaultMoveOnly(int x) : value{x} {
  }

  NonDefaultMoveOnly(NonDefaultMoveOnly const&) = delete;
  auto operator=(NonDefaultMoveOnly const&) -> NonDefaultMoveOnly& = delete;
  NonDefaultMoveOnly(NonDefaultMoveOnly&&) noexcept = default;
  auto operator=(NonDefaultMoveOnly&&) noexcept
    -> NonDefaultMoveOnly& = default;

  int value;
};

} // namespace

TEST("channel sends and receives a single item") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    co_await folly::coro::collectAll(
      [](Sender<int> sender) -> Task<void> {
        co_await sender.send(42);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto value = co_await receiver.recv();
        check_eq(*value, 42);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("channel try_send reports full channel") {
  auto [sender, receiver] = channel<int>(1);
  check(sender.try_send(1).is_ok());
  auto full = sender.try_send(2);
  check(full.is_err());
  check_eq(std::move(full).unwrap_err(), 2);
  folly::coro::blockingWait([&]() -> Task<void> {
    auto value = co_await receiver.recv();
    check_eq(*value, 1);
  }());
}

TEST("channel try_recv reports empty while open") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    auto result = co_await receiver.try_recv();
    check(result.is_err());
    check_eq(std::move(result).unwrap_err(), TryRecvError::empty);
    TENZIR_UNUSED(sender);
  }());
}

TEST("channel try_recv reports closed after drain") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    co_await folly::coro::collectAll(
      [](Sender<int> sender) -> Task<void> {
        co_await sender.send(7);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto value = co_await receiver.recv();
        check_eq(*value, 7);
        auto result = co_await receiver.try_recv();
        check(result.is_err());
        check_eq(std::move(result).unwrap_err(), TryRecvError::closed);
      }());
  }());
}

TEST("channel copied sender keeps channel open until last sender is "
     "destroyed") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    {
      auto sender_copy = sender;
      co_await [](Sender<int> sender) -> Task<void> {
        co_await sender.send(42);
      }(std::move(sender));
      auto value = co_await receiver.recv();
      check_eq(*value, 42);
      auto empty = co_await receiver.try_recv();
      check(empty.is_err());
      check_eq(std::move(empty).unwrap_err(), TryRecvError::empty);
      TENZIR_UNUSED(sender_copy);
    }
    auto none = co_await receiver.recv();
    check(none.is_none());
  }());
}

TEST("channel recv returns none repeatedly after close") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    co_await folly::coro::collectAll(
      [](Sender<int>) -> Task<void> {
        co_return;
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto v1 = co_await receiver.recv();
        check(v1.is_none());
        auto v2 = co_await receiver.recv();
        check(v2.is_none());
        auto v3 = co_await receiver.recv();
        check(v3.is_none());
      }());
  }());
}

TEST("channel works with move-only non-default-constructible types") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<NonDefaultMoveOnly>(1);
    co_await folly::coro::collectAll(
      [](Sender<NonDefaultMoveOnly> sender) -> Task<void> {
        co_await sender.send(NonDefaultMoveOnly{42});
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto value = co_await receiver.recv();
        check_eq(value->value, 42);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("channel wakes multiple blocked receivers on close without queued items") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    auto receiver2 = receiver;
    auto receiver3 = receiver;
    auto entered = std::atomic<size_t>{0};
    co_await folly::coro::collectAll(
      [&](Sender<int> sender) -> Task<void> {
        co_await spawn_blocking([&] {
          while (entered.load(std::memory_order_acquire) != 3) {
            std::this_thread::yield();
          }
        });
        TENZIR_UNUSED(sender);
        co_return;
      }(std::move(sender)),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        auto value = co_await receiver.recv();
        check(value.is_none());
      }(),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        auto value = co_await receiver2.recv();
        check(value.is_none());
      }(),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        auto value = co_await receiver3.recv();
        check(value.is_none());
      }());
  }());
}

TEST("channel drains queued items and then closes across multiple receivers") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(2);
    auto receiver2 = receiver;
    auto receiver3 = receiver;
    auto entered = std::atomic<size_t>{0};
    auto results = std::array<Option<int>, 3>{None{}, None{}, None{}};
    co_await folly::coro::collectAll(
      [&](Sender<int> sender) -> Task<void> {
        co_await spawn_blocking([&] {
          while (entered.load(std::memory_order_acquire) != 3) {
            std::this_thread::yield();
          }
        });
        co_await sender.send(1);
        co_await sender.send(2);
      }(std::move(sender)),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        results[0] = co_await receiver.recv();
      }(),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        results[1] = co_await receiver2.recv();
      }(),
      [&]() -> Task<void> {
        entered.fetch_add(1, std::memory_order_acq_rel);
        results[2] = co_await receiver3.recv();
      }());
    auto values = std::multiset<int>{};
    auto none_count = size_t{0};
    for (auto& result : results) {
      if (result.is_some()) {
        values.insert(*result);
      } else {
        none_count += 1;
      }
    }
    check_eq(values, std::multiset<int>{1, 2});
    check_eq(none_count, size_t{1});
  }());
}

TEST("channel try_recv may continue draining after closure") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(2);
    co_await folly::coro::collectAll(
      [](Sender<int> sender) -> Task<void> {
        co_await sender.send(1);
        co_await sender.send(2);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto first = co_await receiver.try_recv();
        check(first.is_ok());
        auto second = co_await receiver.try_recv();
        check(second.is_ok());
        auto closed = co_await receiver.try_recv();
        check(closed.is_err());
        check_eq(std::move(closed).unwrap_err(), TryRecvError::closed);
      }());
  }());
}

TEST("channel try_recv reports closed repeatedly after close") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = channel<int>(1);
    co_await folly::coro::collectAll(
      [](Sender<int>) -> Task<void> {
        co_return;
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto first = co_await receiver.try_recv();
        check(first.is_err());
        check_eq(std::move(first).unwrap_err(), TryRecvError::closed);
        auto second = co_await receiver.try_recv();
        check(second.is_err());
        check_eq(std::move(second).unwrap_err(), TryRecvError::closed);
      }());
  }());
}

} // namespace tenzir
