//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/exchange.hpp"

#include "tenzir/async/channel.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <set>
#include <vector>

namespace tenzir {

namespace {

/// A `Push` backed by a channel `Sender`. Dropping it closes the channel.
template <class T>
class SenderPush final : public Push<OperatorMsg<T>> {
public:
  explicit SenderPush(Sender<OperatorMsg<T>> sender)
    : sender_{std::move(sender)} {
  }

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    return sender_.send(std::move(x));
  }

private:
  Sender<OperatorMsg<T>> sender_;
};

/// A `Pull` backed by a channel `Receiver`.
template <class T>
class ReceiverPull final : public Pull<OperatorMsg<T>> {
public:
  explicit ReceiverPull(Receiver<OperatorMsg<T>> receiver)
    : receiver_{std::move(receiver)} {
  }

  auto operator()() -> Task<Option<OperatorMsg<T>>> override {
    return receiver_.recv();
  }

private:
  Receiver<OperatorMsg<T>> receiver_;
};

template <class T>
auto local_channel(ChannelId = ChannelId{}, size_t capacity = 128)
  -> PushPull<OperatorMsg<T>> {
  auto [sender, receiver] = channel<OperatorMsg<T>>(capacity);
  return PushPull<OperatorMsg<T>>{
    Box<Push<OperatorMsg<T>>>{SenderPush<T>{std::move(sender)}},
    Box<Pull<OperatorMsg<T>>>{ReceiverPull<T>{std::move(receiver)}},
  };
}

// A channel factory for `make_scatter` / `make_gather`.
auto factory() {
  return [](ChannelId id) {
    return local_channel<int>(std::move(id));
  };
}

// Runs a task body on the global CPU executor. The gather merge loop uses an
// `async_scope`, which spawns lane-pull tasks onto the current executor; those
// tasks must run concurrently with the merger blocking on `next()`, so we need
// a multi-threaded executor rather than the single-threaded `blockingWait` one.
template <class F>
auto run(F&& f) -> void {
  folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getGlobalCPUExecutor(), std::forward<F>(f)()));
}

} // namespace

TEST("scatter round-robins data and broadcasts signals") {
  run([&]() -> Task<void> {
    auto [push, pulls]
      = make_scatter<int>(2, RoundRobinAdaptive{}, factory(), ChannelId{});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        for (auto i = 0; i < 4; ++i) {
          co_await (*push)(OperatorMsg<int>{i});
        }
        co_await (*push)(OperatorMsg<int>{Signal{EndOfData{}}});
        // Drop the scatter to close all lanes.
        push = {};
      }(),
      [&]() -> Task<void> {
        auto lane0 = std::vector<int>{};
        while (auto msg = co_await (*pulls[0])()) {
          if (auto* value = try_as<int>(*msg)) {
            lane0.push_back(*value);
          } else {
            // The signal must be broadcast to this lane.
            check(is<Signal>(*msg));
          }
        }
        // Round-robin sends even indices to lane 0.
        check_eq(lane0, (std::vector<int>{0, 2}));
      }(),
      [&]() -> Task<void> {
        auto lane1 = std::vector<int>{};
        auto got_signal = false;
        while (auto msg = co_await (*pulls[1])()) {
          if (auto* value = try_as<int>(*msg)) {
            lane1.push_back(*value);
          } else {
            got_signal = true;
          }
        }
        check_eq(lane1, (std::vector<int>{1, 3}));
        check(got_signal);
      }());
  });
}

TEST("scatter stops routing data to a closed lane") {
  run([&]() -> Task<void> {
    auto [push, pulls]
      = make_scatter<int>(2, RoundRobinAdaptive{}, factory(), ChannelId{});
    // Retire lane 1 up front.
    static_cast<ScatterPush<int>&>(*push).close_lane(1);
    check_eq(static_cast<ScatterPush<int>&>(*push).open_lanes(), size_t{1});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*push)(OperatorMsg<int>{i});
        }
        push = {};
      }(),
      [&]() -> Task<void> {
        auto lane0 = std::vector<int>{};
        while (auto msg = co_await (*pulls[0])()) {
          lane0.push_back(as<int>(*msg));
        }
        check_eq(lane0, (std::vector<int>{0, 1, 2}));
      }(),
      [&]() -> Task<void> {
        auto count = 0;
        while (auto msg = co_await (*pulls[1])()) {
          ++count;
        }
        check_eq(count, 0);
      }());
  });
}

TEST("gather interleaves data from all lanes") {
  run([&]() -> Task<void> {
    auto parts = make_gather<int>(3, factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*lane)(OperatorMsg<int>{i});
        }
        // Drop the lane to close its channel; otherwise the captured push
        // outlives the coroutine until `collectAll` completes, which never
        // happens because the merger waits for every lane to close.
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        for (auto i = 10; i < 13; ++i) {
          co_await (*lane)(OperatorMsg<int>{i});
        }
        lane = {};
      }(),
      [lane = std::move(parts.lanes[2])]() mutable -> Task<void> {
        for (auto i = 20; i < 23; ++i) {
          co_await (*lane)(OperatorMsg<int>{i});
        }
        lane = {};
      }(),
      [&]() -> Task<void> {
        auto received = std::multiset<int>{};
        while (auto msg = co_await (*parts.pull)()) {
          received.insert(as<int>(*msg));
        }
        auto expected = std::multiset<int>{0, 1, 2, 10, 11, 12, 20, 21, 22};
        check(received == expected);
      }());
  });
}

TEST("gather emits end-of-data exactly once after all lanes deliver it") {
  run([&]() -> Task<void> {
    auto parts = make_gather<int>(2, factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<int>{1});
        co_await (*lane)(OperatorMsg<int>{Signal{EndOfData{}}});
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<int>{2});
        co_await (*lane)(OperatorMsg<int>{Signal{EndOfData{}}});
        lane = {};
      }(),
      [&]() -> Task<void> {
        auto data = 0;
        auto eod = 0;
        while (auto msg = co_await (*parts.pull)()) {
          if (is<int>(*msg)) {
            ++data;
          } else {
            check(is<Signal>(*msg));
            check(is<EndOfData>(as<Signal>(*msg)));
            ++eod;
          }
        }
        check_eq(data, 2);
        check_eq(eod, 1);
      }());
  });
}

TEST("gather drains without end-of-data") {
  run([&]() -> Task<void> {
    auto parts = make_gather<int>(2, factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<int>{5});
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        lane = {};
        co_return;
      }(),
      [&]() -> Task<void> {
        auto received = std::vector<int>{};
        while (auto msg = co_await (*parts.pull)()) {
          received.push_back(as<int>(*msg));
        }
        check_eq(received, (std::vector<int>{5}));
      }());
  });
}

} // namespace tenzir
