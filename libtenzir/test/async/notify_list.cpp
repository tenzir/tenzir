//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/notify_list.hpp"

#include <folly/OperationCancelled.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <utility>

namespace tenzir {

namespace {

/// Whether `signal` has a notification pending, consuming it if so: a wait
/// that is cancelled as soon as it suspends completes only if it never had to.
auto notified(Arc<Notify> signal) -> bool {
  auto cancel = folly::CancellationSource{};
  auto result = false;
  folly::coro::blockingWait(folly::coro::collectAll(
    [&]() -> Task<void> {
      try {
        co_await folly::coro::co_withCancellation(cancel.getToken(),
                                                  signal->wait());
        result = true;
      } catch (folly::OperationCancelled const&) {
      }
    }(),
    [&]() -> Task<void> {
      cancel.requestCancellation();
      co_return;
    }()));
  return result;
}

} // namespace

TEST("notify_all notifies every subscriber") {
  auto list = NotifyList{};
  auto first = list.subscribe();
  auto second = list.subscribe();
  check(not notified(first.signal()));
  list.notify_all();
  check(notified(first.signal()));
  check(notified(second.signal()));
}

TEST("a dropped subscription is no longer notified") {
  auto list = NotifyList{};
  auto kept = list.subscribe();
  auto signal = Arc<Notify>{std::in_place};
  {
    auto dropped = list.subscribe();
    signal = dropped.signal();
  }
  list.notify_all();
  check(notified(kept.signal()));
  check(not notified(signal));
}

TEST("a moved subscription stays registered until its last owner drops it") {
  auto list = NotifyList{};
  auto original = list.subscribe();
  auto signal = original.signal();
  {
    auto moved = std::move(original);
    list.notify_all();
    check(notified(signal));
  }
  list.notify_all();
  check(not notified(signal));
}

} // namespace tenzir
