//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/blocking_executor.hpp"

#include "tenzir/test/test.hpp"

#include <folly/coro/BlockingWait.h>

#include <atomic>
#include <latch>
#include <set>
#include <thread>

namespace tenzir {

TEST("spawn_blocking returns result") {
  auto task = spawn_blocking([] {
    return 42;
  });
  auto result = folly::coro::blockingWait(std::move(task));
  check_eq(result, 42);
}

TEST("spawn_blocking handles void return - direct") {
  auto called = std::atomic<bool>{false};
  auto task = spawn_blocking([&called] {
    called.store(true);
  });
  folly::coro::blockingWait(std::move(task));
  check(called.load());
}

TEST("spawn_blocking handles void return - named") {
  auto called = std::atomic<bool>{false};
  auto fn = [&called] {
    called.store(true);
  };
  auto task = spawn_blocking(std::move(fn));
  folly::coro::blockingWait(std::move(task));
  check(called.load());
}

TEST("spawn_blocking propagates exceptions") {
  auto task = spawn_blocking([]() -> int {
    throw std::runtime_error("test error");
  });
  auto threw = false;
  try {
    folly::coro::blockingWait(std::move(task));
  } catch (const std::runtime_error& e) {
    threw = true;
    check_eq(std::string{e.what()}, std::string{"test error"});
  }
  check(threw);
}

TEST("spawn_blocking runs on different thread") {
  auto main_thread_id = std::this_thread::get_id();
  auto task = spawn_blocking([&] {
    return std::this_thread::get_id();
  });
  auto blocking_thread_id = folly::coro::blockingWait(std::move(task));
  check(main_thread_id != blocking_thread_id);
}

TEST("multiple sequential spawn_blocking calls") {
  // Test that multiple sequential calls work correctly
  auto result0 = folly::coro::blockingWait(spawn_blocking([] {
    return 0;
  }));
  auto result1 = folly::coro::blockingWait(spawn_blocking([] {
    return 1;
  }));
  auto result2 = folly::coro::blockingWait(spawn_blocking([] {
    return 2;
  }));

  check_eq(result0, 0);
  check_eq(result1, 1);
  check_eq(result2, 2);
}

TEST("100 concurrent tasks run on different threads") {
  constexpr auto num_tasks = size_t{100};

  auto thread_ids = std::vector<std::thread::id>(num_tasks);
  auto started = std::latch{static_cast<ptrdiff_t>(num_tasks)};
  auto release = std::latch{1};

  // Spawn all tasks
  auto tasks = std::vector<Task<void>>{};
  tasks.reserve(num_tasks);
  for (auto i = size_t{0}; i < num_tasks; ++i) {
    tasks.push_back(spawn_blocking([&, i] {
      thread_ids[i] = std::this_thread::get_id();
      started.count_down();
      release.wait();
    }));
  }

  // Wait until all tasks are running
  started.wait();

  // All 100 tasks are now running concurrently - verify different thread IDs
  auto unique_ids
    = std::set<std::thread::id>(thread_ids.begin(), thread_ids.end());
  check_eq(unique_ids.size(), num_tasks);

  // Release all tasks and wait for completion
  release.count_down();
  for (auto& task : tasks) {
    folly::coro::blockingWait(std::move(task));
  }
}

} // namespace tenzir
