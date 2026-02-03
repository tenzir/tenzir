#pragma once

#include "tenzir/async/task.hpp"

#include <folly/ExceptionWrapper.h>
#include <folly/Try.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Promise.h>

#include <chrono>
#include <memory>

namespace tenzir {

/// Configuration for the blocking executor (Tokio-aligned defaults).
struct BlockingExecutorConfig {
  size_t max_threads = 512; // Tokio default
  size_t min_threads = 1;
  std::chrono::milliseconds idle_timeout{10000}; // Tokio: 10s
};

/// Global thread pool for blocking operations.
/// Dynamically grows when busy, shrinks when idle.
class BlockingExecutor {
public:
  /// Get the global instance.
  static auto get() -> BlockingExecutor&;

  /// Execute a blocking callable, returning a Task with the result.
  template <class F>
  auto spawn(F&& f) -> Task<std::invoke_result_t<F>>;

  /// Shutdown (for tests/cleanup).
  auto shutdown() -> void;

  ~BlockingExecutor();

  BlockingExecutor(const BlockingExecutor&) = delete;
  BlockingExecutor& operator=(const BlockingExecutor&) = delete;

private:
  friend struct BlockingExecutorSingletonTag;

  explicit BlockingExecutor(BlockingExecutorConfig config = {});

  std::unique_ptr<folly::CPUThreadPoolExecutor> pool_;
};

/// Execute a blocking callable on the global blocking thread pool.
/// Use for file I/O, network operations, subprocess management, etc.
///
/// Example:
///   auto result = co_await spawn_blocking([&] {
///     return blocking_file_read(path);
///   });
template <class F>
auto spawn_blocking(F&& f) -> Task<std::invoke_result_t<F>> {
  return BlockingExecutor::get().spawn(std::forward<F>(f));
}

// Template implementation
template <class F>
auto BlockingExecutor::spawn(F&& f) -> Task<std::invoke_result_t<F>> {
  using R = std::invoke_result_t<F>;
  // Folly futures don't support void directly, use Unit instead.
  using FutureType = std::conditional_t<std::is_void_v<R>, folly::Unit, R>;
  auto [promise, future] = folly::makePromiseContract<FutureType>();

  pool_->add([func = std::forward<F>(f), p = std::move(promise)]() mutable {
    try {
      if constexpr (std::is_void_v<R>) {
        std::invoke(std::move(func));
        p.setValue(folly::Unit{});
      } else {
        p.setValue(std::invoke(std::move(func)));
      }
    } catch (...) {
      p.setException(folly::exception_wrapper{std::current_exception()});
    }
  });

  // co_await on SemiFuture returns the value
  if constexpr (std::is_void_v<R>) {
    co_await std::move(future);
  } else {
    co_return co_await std::move(future);
  }
}

} // namespace tenzir
