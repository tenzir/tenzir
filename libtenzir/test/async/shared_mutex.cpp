//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/shared_mutex.hpp"

#include "tenzir/async/task.hpp"

#include <folly/coro/BlockingWait.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <optional>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

// Confirms that both lock flavors still work on a mutex after `mutex` has been
// moved into its final location.
auto exercise(RawSharedMutex& mutex) -> Task<void> {
  {
    auto guard = co_await mutex.unique_lock();
  }
  {
    auto guard = co_await mutex.shared_lock();
  }
  // Multiple concurrent shared locks should coexist.
  {
    auto a = co_await mutex.shared_lock();
    auto b = co_await mutex.shared_lock();
  }
  // And a unique lock must be obtainable again once the readers drain.
  {
    auto guard = co_await mutex.unique_lock();
  }
  co_return;
}

} // namespace

TEST("RawSharedMutex move construction preserves a working mutex") {
  folly::coro::blockingWait([]() -> Task<void> {
    auto original = RawSharedMutex{};
    auto moved = std::move(original);
    co_await exercise(moved);
  }());
}

TEST("RawSharedMutex move assignment preserves a working mutex") {
  folly::coro::blockingWait([]() -> Task<void> {
    auto source = RawSharedMutex{};
    auto target = RawSharedMutex{};
    target = std::move(source);
    co_await exercise(target);
  }());
}

TEST("RawSharedMutex can be moved into place while idle") {
  folly::coro::blockingWait([]() -> Task<void> {
    auto storage = std::vector<RawSharedMutex>{};
    storage.emplace_back();
    // Force a reallocation so the elements are relocated via move.
    storage.reserve(storage.capacity() + 1);
    storage.emplace_back();
    co_await exercise(storage.front());
    co_await exercise(storage.back());
    auto opt = std::optional<RawSharedMutex>{};
    opt.emplace();
    co_await exercise(*opt);
  }());
}

TEST("SharedMutex is movable and keeps its value") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto original = SharedMutex<int>{42};
    auto moved = std::move(original);
    {
      auto guard = co_await moved.unique_lock();
      check_eq(*guard, 42);
      *guard = 7;
    }
    {
      auto guard = co_await moved.shared_lock();
      check_eq(*guard, 7);
    }
  }());
}

} // namespace tenzir
