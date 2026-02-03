#include "tenzir/async/blocking_executor.hpp"

#include <folly/executors/thread_factory/NamedThreadFactory.h>

namespace tenzir {

auto BlockingExecutor::get() -> BlockingExecutor& {
  static BlockingExecutor instance;
  return instance;
}

BlockingExecutor::BlockingExecutor(BlockingExecutorConfig config)
  : pool_{std::make_unique<folly::CPUThreadPoolExecutor>(
      std::make_pair(config.max_threads, config.min_threads),
      folly::CPUThreadPoolExecutor::makeLifoSemQueue(),
      std::make_shared<folly::NamedThreadFactory>("TenzirBlocking"))} {
  pool_->setThreadDeathTimeout(config.idle_timeout);
}

BlockingExecutor::~BlockingExecutor() {
  shutdown();
}

auto BlockingExecutor::shutdown() -> void {
  if (pool_) {
    pool_->join();
  }
}

} // namespace tenzir
