//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/broadcast.hpp"

#include "tenzir/async/result.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/executors/ManualExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <barrier>
#include <thread>

namespace tenzir {

TEST("broadcast bounds receivers independently and delivers final values") {
  auto broadcast = Broadcast<int>{2};
  auto slow = broadcast.subscribe();
  auto fast = broadcast.subscribe();
  for (auto i = 0; i < 6; ++i) {
    broadcast.publish(i);
    check_eq(folly::coro::blockingWait(fast.recv()).unwrap(), i);
  }
  broadcast.close(99);
  check_eq(folly::coro::blockingWait(slow.recv()).unwrap(), 0);
  check_eq(folly::coro::blockingWait(slow.recv()).unwrap(), 1);
  check_eq(folly::coro::blockingWait(slow.recv()).unwrap(), 99);
  check_eq(folly::coro::blockingWait(fast.recv()).unwrap(), 99);
  check(not folly::coro::blockingWait(slow.recv()));
  check(not folly::coro::blockingWait(fast.recv()));
  auto late = broadcast.subscribe();
  check_eq(folly::coro::blockingWait(late.recv()).unwrap(), 99);
  check(not folly::coro::blockingWait(late.recv()));
}

TEST("broadcast receiver replacement releases idle subscriptions and "
     "payloads") {
  auto first = Broadcast<std::shared_ptr<int>>{2};
  auto second = Broadcast<std::shared_ptr<int>>{2};
  auto receiver = first.subscribe();
  auto payload = std::make_shared<int>(42);
  auto retained = std::weak_ptr<int>{payload};
  first.publish(payload);
  payload.reset();
  check(not retained.expired());
  receiver = second.subscribe();
  check(not first.has_receivers());
  check(second.has_receivers());
  check(retained.expired());
  auto moved = std::move(receiver);
  check(not folly::coro::blockingWait(receiver.recv()));
  moved = {};
  check(not second.has_receivers());
}

TEST("destroying a broadcaster closes receivers after accepted data") {
  auto receiver = Broadcast<int>::Receiver{};
  {
    auto broadcast = Broadcast<int>{2};
    receiver = broadcast.subscribe();
    broadcast.publish(7);
  }
  check_eq(folly::coro::blockingWait(receiver.recv()).unwrap(), 7);
  check(not folly::coro::blockingWait(receiver.recv()));
}

TEST("cancelling broadcast receive detaches before or during suspension") {
  for (auto precancelled : {false, true}) {
    auto broadcast = Broadcast<int>{2};
    auto receiver = broadcast.subscribe();
    auto executor = folly::ManualExecutor{};
    auto cancellation = folly::CancellationSource{};
    if (precancelled) {
      cancellation.requestCancellation();
    }
    auto receiving = folly::coro::co_withCancellation(cancellation.getToken(),
                                                      receiver.recv());
    auto pending
      = folly::coro::co_withExecutor(folly::getKeepAliveToken(&executor),
                                     catch_cancellation(std::move(receiving)))
          .start();
    auto cleanup = detail::scope_guard{[&]() noexcept {
      cancellation.requestCancellation();
      executor.drain();
    }};
    executor.drain();
    if (not precancelled) {
      require(not pending.isReady());
      cancellation.requestCancellation();
      executor.drain();
    }
    require(pending.isReady());
    check(not std::move(pending).get());
    check(not broadcast.has_receivers());
    check(not folly::coro::blockingWait(receiver.recv()));
  }
}

TEST("latest broadcast coalesces while a notified receiver is stalled") {
  auto broadcast = Broadcast<int, BroadcastMode::latest>{};
  auto receiver = broadcast.subscribe();
  auto executor = folly::ManualExecutor{};
  auto pending = folly::coro::co_withExecutor(
                   folly::getKeepAliveToken(&executor), receiver.recv())
                   .start();
  executor.drain();
  require(not pending.isReady());
  for (auto i = 0; i < 100; ++i) {
    broadcast.publish(i);
  }
  broadcast.close(100);
  executor.drain();
  require(pending.isReady());
  auto value = std::move(pending).get().unwrap();
  if (value != 100) {
    check(value >= 0 and value < 100);
    check_eq(folly::coro::blockingWait(receiver.recv()).unwrap(), 100);
  }
  check(not folly::coro::blockingWait(receiver.recv()));
}

TEST("latest broadcast delivers closure requested inline by a receiver") {
  auto broadcast = Broadcast<int, BroadcastMode::latest>{};
  auto first = broadcast.subscribe();
  auto second = broadcast.subscribe();
  auto publishing = false;
  auto close_after_receive = [&]() -> Task<void> {
    check_eq((co_await first.recv()).unwrap(), 7);
    check(publishing);
    broadcast.close(99);
  };
  auto pending = folly::coro::co_withExecutor(
                   folly::getKeepAliveToken(&folly::InlineExecutor::instance()),
                   close_after_receive())
                   .start();
  auto cleanup = detail::scope_guard{[&]() noexcept {
    broadcast.close(99);
  }};
  require(not pending.isReady());
  publishing = true;
  broadcast.publish(7);
  publishing = false;
  require(pending.isReady());
  std::move(pending).get();
  for (auto* receiver : {&first, &second}) {
    check_eq(folly::coro::blockingWait(receiver->recv()).unwrap(), 99);
    check(not folly::coro::blockingWait(receiver->recv()));
  }
}

TEST("latest broadcast closure cannot be overtaken by concurrent publication") {
  for (auto iteration = 0; iteration < 32; ++iteration) {
    auto broadcast = Broadcast<int, BroadcastMode::latest>{};
    auto receiver = broadcast.subscribe();
    auto start = std::barrier{2};
    auto publisher = std::jthread{[&] {
      start.arrive_and_wait();
      for (auto i = 0; i < 100; ++i) {
        broadcast.publish(i);
      }
    }};
    start.arrive_and_wait();
    broadcast.close(-1);
    publisher.join();
    check_eq(folly::coro::blockingWait(receiver.recv()).unwrap(), -1);
    check(not folly::coro::blockingWait(receiver.recv()));
  }
}

TEST("broadcast supports concurrent publishers without losing accepted "
     "values") {
  auto broadcast = Broadcast<int>{200};
  auto receiver = broadcast.subscribe();
  auto start = std::barrier{3};
  auto publish = [&](int base) {
    start.arrive_and_wait();
    for (auto i = 0; i < 100; ++i) {
      broadcast.publish(base + i);
    }
  };
  auto first = std::jthread{publish, 0};
  auto second = std::jthread{publish, 100};
  start.arrive_and_wait();
  first.join();
  second.join();
  broadcast.close(-1);
  auto values = std::vector<int>{};
  while (auto value = folly::coro::blockingWait(receiver.recv())) {
    values.push_back(*value);
  }
  require_eq(values.size(), 201u);
  check_eq(values.back(), -1);
  values.pop_back();
  std::ranges::sort(values);
  for (auto i = 0; i < 200; ++i) {
    check_eq(values[i], i);
  }
}

} // namespace tenzir
