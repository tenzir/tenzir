//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ai.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/CurrentExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <algorithm>
#include <numeric>
#include <vector>

using namespace tenzir;

namespace {

struct Observed {
  std::vector<size_t> started;
  size_t max_active = 0;
};

/// Runs `request_all` with requests that yield a few times while in flight,
/// so that concurrent requests overlap.
auto run(size_t count, uint64_t concurrency) -> Observed {
  auto result = Observed{};
  auto active = size_t{};
  folly::coro::blockingWait(
    ai::request_all(count, concurrency, [&](size_t i) -> Task<void> {
      result.started.push_back(i);
      ++active;
      result.max_active = std::max(result.max_active, active);
      for (auto j = 0; j < 3; ++j) {
        co_await folly::coro::co_reschedule_on_current_executor;
      }
      --active;
    }));
  return result;
}

auto indices(size_t count) -> std::vector<size_t> {
  auto result = std::vector<size_t>(count);
  std::iota(result.begin(), result.end(), size_t{});
  return result;
}

} // namespace

TEST("request_all runs every request once with bounded concurrency") {
  auto observed = run(50, 3);
  CHECK_EQUAL(observed.max_active, size_t{3});
  std::ranges::sort(observed.started);
  CHECK_EQUAL(observed.started, indices(50));
}

TEST("request_all runs requests one at a time") {
  auto observed = run(10, 1);
  CHECK_EQUAL(observed.max_active, size_t{1});
  CHECK_EQUAL(observed.started, indices(10));
}

TEST("request_all handles fewer requests than permits") {
  auto observed = run(2, 8);
  CHECK_EQUAL(observed.max_active, size_t{2});
  std::ranges::sort(observed.started);
  CHECK_EQUAL(observed.started, indices(2));
  CHECK(run(0, 4).started.empty());
}
