//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/stop_flag.hpp"

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/coro/WithCancellation.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

namespace tenzir {

namespace {

/// Never completes on its own, and records whether it was cancelled.
auto forever(bool& cancelled) -> Task<int> {
  try {
    co_await wait_forever();
  } catch (folly::OperationCancelled const&) {
    cancelled = true;
    throw;
  }
  co_return 0;
}

} // namespace

TEST("a stop requested on one copy is requested on all") {
  auto stop = StopFlag{};
  auto copy = stop;
  check(not copy.is_requested());
  stop.request();
  check(copy.is_requested());
  // A requested stop stays requested.
  stop.request();
  check(stop.is_requested());
}

TEST("wait completes once the stop is requested") {
  auto stop = StopFlag{};
  folly::coro::blockingWait(
    folly::coro::collectAll(stop.wait(), [&]() -> Task<void> {
      stop.request();
      co_return;
    }()));
  // And at once after that.
  folly::coro::blockingWait(stop.wait());
}

TEST("a cancelled wait propagates the cancellation") {
  auto stop = StopFlag{};
  auto cancel = folly::CancellationSource{};
  cancel.requestCancellation();
  auto cancelled = false;
  folly::coro::blockingWait([&]() -> Task<void> {
    try {
      co_await folly::coro::co_withCancellation(cancel.getToken(), stop.wait());
    } catch (folly::OperationCancelled const&) {
      cancelled = true;
    }
  }());
  check(cancelled);
  check(not stop.is_requested());
}

TEST("run returns what completes before the stop") {
  auto stop = StopFlag{};
  auto result = folly::coro::blockingWait(stop.run([]() -> Task<int> {
    co_return 42;
  }()));
  check(result.is_some() and *result == 42);
  auto unit = folly::coro::blockingWait(stop.run([]() -> Task<void> {
    co_return;
  }()));
  check(unit.is_some());
}

TEST("run cancels the work and returns none when stopped") {
  auto stop = StopFlag{};
  auto cancelled = false;
  auto [result, _] = folly::coro::blockingWait(
    folly::coro::collectAll(stop.run(forever(cancelled)), [&]() -> Task<void> {
      stop.request();
      co_return;
    }()));
  check(result.is_none());
  check(cancelled);
}

TEST("run returns none at once when the stop was requested before") {
  auto stop = StopFlag{};
  stop.request();
  auto cancelled = false;
  auto result = folly::coro::blockingWait(stop.run(forever(cancelled)));
  check(result.is_none());
}

TEST("run propagates the caller's cancellation, also over a stop") {
  for (auto also_stop : {false, true}) {
    auto stop = StopFlag{};
    auto cancel = folly::CancellationSource{};
    auto work_cancelled = false;
    auto propagated = false;
    folly::coro::blockingWait([&]() -> Task<void> {
      try {
        co_await folly::coro::collectAll(
          folly::coro::co_withCancellation(cancel.getToken(),
                                           stop.run(forever(work_cancelled))),
          [&]() -> Task<void> {
            if (also_stop) {
              stop.request();
            }
            cancel.requestCancellation();
            co_return;
          }());
      } catch (folly::OperationCancelled const&) {
        propagated = true;
      }
    }());
    check(work_cancelled);
    check(propagated);
  }
}

TEST("run propagates a cancellation that the work brought about itself") {
  auto stop = StopFlag{};
  auto propagated = false;
  folly::coro::blockingWait([&]() -> Task<void> {
    try {
      std::ignore = co_await stop.run([]() -> Task<int> {
        auto own = folly::CancellationSource{};
        own.requestCancellation();
        co_await folly::coro::co_withCancellation(own.getToken(),
                                                  wait_forever());
        co_return 0;
      }());
    } catch (folly::OperationCancelled const&) {
      propagated = true;
    }
  }());
  check(propagated);
  check(not stop.is_requested());
}

TEST("a child is stopped with its parent, but not the other way") {
  auto parent = StopFlag{};
  auto child = parent.child();
  child.request();
  check(child.is_requested());
  check(not parent.is_requested());
  auto other = parent.child();
  parent.request();
  check(other.is_requested());
  // A child of a stopped parent starts out stopped.
  check(parent.child().is_requested());
}

TEST("request_on_exit requests the stop when the guard goes") {
  auto stop = StopFlag{};
  {
    auto guard = stop.request_on_exit();
    check(not stop.is_requested());
  }
  check(stop.is_requested());
}

} // namespace tenzir
