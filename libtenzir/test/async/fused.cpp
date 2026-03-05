//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/fused.hpp"

#include <caf/test/test.hpp>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

namespace tenzir {

// In all tests, the sender must be moved into its coroutine so it gets
// destroyed when the coroutine ends. The receiver must always do a final
// recv() to ack the last item. This unblocks the sender, which then finishes
// and is destroyed, causing the final recv() to return None.

TEST("fused channel sends and receives a single item") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<int>();
    co_await folly::coro::collectAll(
      [](FusedSender<int> s) -> Task<void> {
        co_await s.send(42);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto value = co_await receiver.recv();
        check_eq(*value, 42);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("fused channel sends and receives multiple items") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<int>();
    co_await folly::coro::collectAll(
      [](FusedSender<int> s) -> Task<void> {
        co_await s.send(1);
        co_await s.send(2);
        co_await s.send(3);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto v1 = co_await receiver.recv();
        check_eq(*v1, 1);
        auto v2 = co_await receiver.recv();
        check_eq(*v2, 2);
        auto v3 = co_await receiver.recv();
        check_eq(*v3, 3);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("fused channel returns none when sender is destroyed") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<int>();
    co_await folly::coro::collectAll(
      [](FusedSender<int> s) -> Task<void> {
        co_await s.send(1);
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto v = co_await receiver.recv();
        check_eq(*v, 1);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("fused channel returns none on repeated recv after sender destroyed") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<int>();
    co_await folly::coro::collectAll(
      [](FusedSender<int>) -> Task<void> {
        co_return;
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto v1 = co_await receiver.recv();
        check(v1.is_none());
        auto v2 = co_await receiver.recv();
        check(v2.is_none());
      }());
  }());
}

TEST("fused channel sender blocks until receiver calls recv again") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<int>();
    auto log = std::vector<std::string>{};
    co_await folly::coro::collectAll(
      [&log](FusedSender<int> s) -> Task<void> {
        log.emplace_back("send-1-begin");
        co_await s.send(1);
        log.emplace_back("send-1-end");
        log.emplace_back("send-2-begin");
        co_await s.send(2);
        log.emplace_back("send-2-end");
      }(std::move(sender)),
      [&]() -> Task<void> {
        log.emplace_back("recv-1-begin");
        auto v1 = co_await receiver.recv();
        check_eq(*v1, 1);
        log.emplace_back("recv-1-end");
        log.emplace_back("recv-2-begin");
        auto v2 = co_await receiver.recv();
        check_eq(*v2, 2);
        log.emplace_back("recv-2-end");
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
    // send(1) blocks until recv() is called again (acknowledging the item).
    auto send_1_end = std::ranges::find(log, "send-1-end");
    auto recv_2_begin = std::ranges::find(log, "recv-2-begin");
    check(send_1_end != log.end());
    check(recv_2_begin != log.end());
    check(recv_2_begin < send_1_end);
  }());
}

TEST("fused channel works with move-only types") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [sender, receiver] = fused_channel<std::unique_ptr<int>>();
    co_await folly::coro::collectAll(
      [](FusedSender<std::unique_ptr<int>> s) -> Task<void> {
        co_await s.send(std::make_unique<int>(42));
      }(std::move(sender)),
      [&]() -> Task<void> {
        auto value = co_await receiver.recv();
        check_eq(**value, 42);
        auto none = co_await receiver.recv();
        check(none.is_none());
      }());
  }());
}

TEST("fused push pull wrapper works") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto [push, pull] = fused_channel<int>().into_push_pull();
    co_await folly::coro::collectAll(
      [](Box<Push<int>> p) -> Task<void> {
        co_await p(10);
        co_await p(20);
      }(std::move(push)),
      [&]() -> Task<void> {
        auto v1 = co_await pull();
        check_eq(*v1, 10);
        auto v2 = co_await pull();
        check_eq(*v2, 20);
        auto none = co_await pull();
        check(none.is_none());
      }());
  }());
}

} // namespace tenzir
