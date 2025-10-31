//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <folly/coro/Task.h>
#include <folly/fibers/Semaphore.h>

namespace tenzir {

class Notify {
public:
  auto viaIfAsync(folly::Executor::KeepAlive<> executor) {
    return folly::coro::co_withExecutor(std::move(executor),
                                        semaphore_.co_wait());
  }

  void notify_one() {
    // TODO: This is quite bad, and there is a race where we could notify more
    // than one waiter. However, we can't just use `folly::coro::Baton` since
    // that can not be cancelled!
    auto tokens = semaphore_.getAvailableTokens();
    if (tokens == 0) {
      semaphore_.signal();
    }
  }

private:
  folly::fibers::Semaphore semaphore_{0};
};

} // namespace tenzir
