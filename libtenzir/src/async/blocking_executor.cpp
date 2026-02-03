#include "tenzir/async/blocking_executor.hpp"

#include <folly/Singleton.h>
#include <folly/executors/thread_factory/NamedThreadFactory.h>

namespace tenzir {

struct BlockingExecutorSingletonTag {
  static auto create() -> BlockingExecutor* {
    return new BlockingExecutor{};
  }
};

namespace {
folly::Singleton<BlockingExecutor>
  gBlockingExecutor(BlockingExecutorSingletonTag::create);
} // namespace

auto BlockingExecutor::get() -> BlockingExecutor& {
  return *gBlockingExecutor.try_get();
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
