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

auto run_secret_cache_cleanup(SharedMutex<SecretCache>& state) -> Task<void> {
  auto timekeeper = folly::ThreadWheelTimekeeper{};
  while (true) {
    co_await folly::coro::co_safe_point;
    auto wait = duration{};
    {
      auto cache = co_await state.unique_lock();
      wait = cache->cleanup();
    }
    co_await folly::coro::sleep(
      std::chrono::duration_cast<folly::HighResDuration>(wait), &timekeeper);
  }
}

} // namespace

folly::CancellationSource SecretCache::cancel_source{};

auto SecretCache::lookup(std::string_view key, time now) const
  -> Option<ecc::cleansing_blob> {
  auto it = data_.find(key);
  if (it == data_.end()) {
    return None{};
  }
  auto& [value, last_used] = it->second;
  if (now - last_used.load(std::memory_order_relaxed) < lifetime_) {
    if (refresh_on_read_) {
      last_used.store(now, std::memory_order_relaxed);
    }
    return value;
  }
  return None{};
}

auto SecretCache::insert(std::string key, ecc::cleansing_blob value, time now)
  -> void {
  if (background_cleanup_) {
    start_cleanup();
  } else {
    std::ignore = cleanup(now);
  }
  const auto [it, inserted]
    = data_.try_emplace(std::move(key), std::move(value), now);
  if (inserted) {
    return;
  }
  auto& entry = it->second;
  entry.value = std::move(value);
  entry.last_used = now;
}

auto SecretCache::cleanup(time now) -> duration {
  auto const cutoff = now - lifetime_;
  auto wait = duration{lifetime_};
  for (auto it = data_.begin(); it != data_.end();) {
    const auto& [_, entry] = *it;
    auto last_used = entry.last_used.load(std::memory_order_relaxed);
    if (last_used <= cutoff) {
      it = data_.erase(it);
      continue;
    }
    auto const age = (now - last_used);
    auto const entry_wait = lifetime_ - age;
    if (entry_wait < wait) {
      wait = entry_wait;
    }
    ++it;
  }
  return wait;
}

auto SecretCache::clear() -> void {
  data_.clear();
  ++generation_;
}

auto SecretCache::erase(std::string_view key) -> void {
  if (auto it = data_.find(key); it != data_.end()) {
    data_.erase(it);
  }
  ++generation_;
}

auto SecretStore::run_cache_cleanup() -> Task<void> {
  co_await run_secret_cache_cleanup(*cache_);
}

auto SecretStore::clear_cache() -> Task<void> {
  auto cache = co_await cache_->unique_lock();
  cache->clear();
}

auto SecretStore::forget(std::vector<std::string> names) -> Task<void> {
  auto cache = co_await cache_->unique_lock();
  for (auto const& name : names) {
    cache->erase(name);
  }
}

auto SecretCache::instance() -> SharedMutex<SecretCache>& {
  static auto instance = SharedMutex<SecretCache>{std::in_place, true};
  return instance;
}

auto SecretCache::start_cleanup() -> void {
  static auto once = std::once_flag{};
  std::call_once(once, [] {
    folly::coro::co_withCancellation(
      cancel_source.getToken(),
      folly::coro::co_withExecutor(folly::getGlobalCPUExecutor(),
                                   run_secret_cache_cleanup(instance())))
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
