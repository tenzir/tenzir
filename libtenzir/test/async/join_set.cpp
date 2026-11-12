//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/join_set.hpp"

#include "tenzir/async/select_set.hpp"
#include "tenzir/async/task.hpp"

#include <folly/OperationCancelled.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <algorithm>
#include <cstddef>
#include <numeric>
#include <vector>

namespace tenzir {

namespace {

// Large enough that the completion queue crosses many segments.
constexpr auto task_count = 1000;

auto iota_vector(int count) -> std::vector<int> {
  auto result = std::vector<int>(static_cast<size_t>(count));
  std::iota(result.begin(), result.end(), 0);
  return result;
}

} // namespace

TEST("join_set yields every result when completions cross segments") {
  auto results = folly::coro::blockingWait([&]() -> Task<std::vector<int>> {
    auto set = JoinSet<int>{};
    auto results = std::vector<int>{};
    co_await set.activate([&] -> Task<void> {
      for (auto i = 0; i < task_count; ++i) {
        set.add([i] -> Task<int> {
          co_return i;
        });
      }
      while (auto value = co_await set.next()) {
        results.push_back(*value);
      }
      check_eq(set.running(), size_t{0});
    });
    co_return results;
  }());
  std::ranges::sort(results);
  check_eq(results, iota_vector(task_count));
}

TEST("join_set yields every result from concurrent completions") {
  auto pool = folly::CPUThreadPoolExecutor{4};
  auto results = folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getKeepAliveToken(pool), [&]() -> Task<std::vector<int>> {
      auto set = JoinSet<int>{};
      auto results = std::vector<int>{};
      co_await set.activate([&] -> Task<void> {
        for (auto i = 0; i < task_count; ++i) {
          set.add([i] -> Task<int> {
            co_await folly::coro::co_reschedule_on_current_executor;
            co_return i;
          });
        }
        while (auto value = co_await set.next()) {
          results.push_back(*value);
        }
      });
      co_return results;
    }()));
  std::ranges::sort(results);
  check_eq(results, iota_vector(task_count));
}

TEST("join_set skips cancelled tasks") {
  auto results = folly::coro::blockingWait([&]() -> Task<std::vector<int>> {
    auto set = JoinSet<int>{};
    auto results = std::vector<int>{};
    co_await set.activate([&] -> Task<void> {
      for (auto i = 0; i < task_count; ++i) {
        set.add([i] -> Task<int> {
          if (i % 2 == 0) {
            throw folly::OperationCancelled{};
          }
          co_return i;
        });
      }
      while (auto value = co_await set.next()) {
        results.push_back(*value);
      }
    });
    co_return results;
  }());
  std::ranges::sort(results);
  auto expected = std::vector<int>{};
  for (auto i = 1; i < task_count; i += 2) {
    expected.push_back(i);
  }
  check_eq(results, expected);
}

TEST("select_set buffers unmatched results and returns them later") {
  auto [first, rest] = folly::coro::blockingWait(
    [&]() -> Task<std::pair<int, std::vector<int>>> {
      auto set = SelectSet<int>{};
      auto first = -1;
      auto rest = std::vector<int>{};
      co_await set.activate([&] -> Task<void> {
        for (auto i = 0; i < task_count; ++i) {
          set.add([i] -> Task<int> {
            co_return i;
          });
        }
        // Waiting for the last value buffers everything that completes first.
        auto last = co_await set.next([](int x) {
          return x == task_count - 1;
        });
        require(last.is_some());
        first = *last;
        while (auto value = co_await set.next([](int) {
          return true;
        })) {
          rest.push_back(*value);
        }
      });
      co_return {first, std::move(rest)};
    }());
  check_eq(first, task_count - 1);
  std::ranges::sort(rest);
  check_eq(rest, iota_vector(task_count - 1));
}

} // namespace tenzir
