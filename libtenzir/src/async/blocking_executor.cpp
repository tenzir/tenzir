//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/blocking_executor.hpp"

#include <folly/executors/thread_factory/NamedThreadFactory.h>

namespace tenzir {

auto BlockingExecutor::get() -> BlockingExecutor& {
  static BlockingExecutor instance;
  return instance;
}

BlockingExecutor::BlockingExecutor(BlockingExecutorConfig config)
  : pool_{std::make_pair(config.max_threads, config.min_threads),
          folly::CPUThreadPoolExecutor::makeLifoSemQueue(),
          std::make_shared<folly::NamedThreadFactory>("TenzirBlocking")} {
  pool_.setThreadDeathTimeout(config.idle_timeout);
}

BlockingExecutor::~BlockingExecutor() {
  pool_.join();
}

} // namespace tenzir
