//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async_secret_resolution.hpp"

#include "tenzir/async/task.hpp"

#include <folly/coro/Sleep.h>
#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/ThreadWheelTimekeeper.h>

#include <chrono>
#include <mutex>

namespace tenzir {

namespace {

auto run_secret_cache_cleanup() -> Task<void> {
  auto timekeeper = folly::ThreadWheelTimekeeper{};
  while (true) {
    co_await folly::coro::co_safe_point;
    auto wait = duration{};
    {
      auto cache = co_await SecretCache::instance().unique_lock();
      wait = cache->cleanup();
    }
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(wait), &timekeeper);
  }
}

} // namespace

folly::CancellationSource SecretCache::cancel_source{};

auto SecretCache::lookup(std::string_view key) const
  -> Option<ecc::cleansing_blob> {
  auto it = data_.find(key);
  if (it == data_.end()) {
    return None{};
  }
  auto& [value, last_used] = it->second;
  last_used = time::clock::now();
  return value;
}

auto SecretCache::insert(std::string key, ecc::cleansing_blob value) -> void {
  data_.try_emplace(std::move(key), std::move(value), time::clock::now());
  start_cleanup();
}

auto SecretCache::cleanup() -> duration {
  auto const now = time::clock::now();
  auto const cutoff = now - ttl;
  auto wait = duration{ttl};
  for (auto it = data_.begin(); it != data_.end();) {
    const auto& [_, entry] = *it;
    auto last_used = entry.last_used.load(std::memory_order_relaxed);
    if (last_used < cutoff) {
      it = data_.erase(it);
      continue;
    }
    auto const age = (now - last_used);
    auto const entry_wait = ttl - age;
    if (entry_wait < wait) {
      wait = entry_wait;
    }
    ++it;
  }
  return wait;
}

auto SecretCache::instance() -> SharedMutex<SecretCache>& {
  static auto instance = SharedMutex<SecretCache>{};
  return instance;
}

auto SecretCache::start_cleanup() -> void {
  static auto once = std::once_flag{};
  std::call_once(once, [] {
    folly::coro::co_withCancellation(
      cancel_source.getToken(),
      folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
                                   run_secret_cache_cleanup()))
      .start([](auto&& result) {
        if (result.template hasException<folly::OperationCancelled>()) {
          return;
        }
        if (result.hasException()) {
          result.exception().throw_exception();
        }
      });
  });
}

} // namespace tenzir
