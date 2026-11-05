//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/published.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <utility>
#include <vector>

namespace tenzir {

namespace {

auto read(Published<int>& published) -> int {
  return *folly::coro::blockingWait(published.get());
}

} // namespace

TEST("a published update replaces the value") {
  auto published = Published<int>{1};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto update = co_await published.begin_update();
    check_eq(update.current(), 1);
    update.next() = 2;
    co_await std::move(update).publish();
  }());
  check_eq(read(published), 2);
}

TEST("a dropped update changes nothing") {
  auto published = Published<int>{1};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto update = co_await published.begin_update();
    update.next() = 2;
  }());
  check_eq(read(published), 1);
}

TEST("readers see the last value while an update is open") {
  auto published = Published<int>{1};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto update = co_await published.begin_update();
    update.next() = 2;
    check_eq(*co_await published.get(), 1);
    co_await std::move(update).publish();
    check_eq(*co_await published.get(), 2);
  }());
}

TEST("an untouched update keeps the published snapshot") {
  auto published = Published<int>{1};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto before = co_await published.get();
    auto update = co_await published.begin_update();
    co_await std::move(update).publish();
    auto after = co_await published.get();
    check_eq(&*after, &*before);
  }());
}

TEST("a publish reports the old and the new value") {
  auto reported = std::vector<std::pair<int, int>>{};
  auto published = Published<int>{1, [&](int const& old, int const& next) {
                                    reported.emplace_back(old, next);
                                  }};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto untouched = co_await published.begin_update();
    co_await std::move(untouched).publish();
    auto update = co_await published.begin_update();
    update.next() = 2;
    co_await std::move(update).publish();
  }());
  check_eq(reported.size(), size_t{1});
  check_eq(reported[0].first, 1);
  check_eq(reported[0].second, 2);
}

TEST("updates wait for each other") {
  auto published = Published<int>{0};
  folly::coro::blockingWait([&]() -> Task<void> {
    auto first = co_await published.begin_update();
    auto second_started = false;
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        auto second = co_await published.begin_update();
        second_started = true;
        // Starts from what the first one published.
        check_eq(second.current(), 1);
        second.next() = 2;
        co_await std::move(second).publish();
      }(),
      [&]() -> Task<void> {
        co_await folly::coro::co_reschedule_on_current_executor;
        check(not second_started);
        first.next() = 1;
        co_await std::move(first).publish();
      }());
    check(second_started);
  }());
  check_eq(read(published), 2);
}

} // namespace tenzir
