//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/mutex.hpp"
#include "tenzir/async/semaphore.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/option.hpp"

#include <chrono>
#include <list>
#include <string>
#include <unordered_map>

namespace tenzir {

/// Configuration for reverse DNS lookups.
struct ReverseDnsConfig {
  /// Maximum number of concurrent blocking lookups.
  size_t max_in_flight = 32;

  /// Maximum number of cached PTR results.
  size_t max_entries = 4096;

  /// Cache duration for successful PTR lookups.
  std::chrono::seconds positive_ttl{300};

  /// Cache duration for missing PTR records or lookup failures.
  std::chrono::seconds negative_ttl{30};
};

enum class ReverseDnsStatus {
  resolved,
  not_found,
  failed,
};

/// Result of a reverse DNS lookup.
struct ReverseDnsResult {
  ReverseDnsStatus status = ReverseDnsStatus::not_found;
  Option<std::string> hostname = None{};
  Option<std::string> error = None{};
};

/// Async reverse-DNS resolver with caching and bounded concurrency.
///
/// This helper exposes an async API, but currently implements lookups via
/// `spawn_blocking(...)` and `getnameinfo(3)` under the hood. This means that
/// lookups do not block the caller's coroutine or an EventBase thread, but the
/// underlying libc call itself is still blocking.
class ReverseDnsResolver {
public:
  explicit ReverseDnsResolver(ReverseDnsConfig config = {});

  ReverseDnsResolver(ReverseDnsResolver const&) = delete;
  auto operator=(ReverseDnsResolver const&) -> ReverseDnsResolver& = delete;
  ReverseDnsResolver(ReverseDnsResolver&&) noexcept = default;
  auto operator=(ReverseDnsResolver&&) noexcept -> ReverseDnsResolver&
    = default;
  ~ReverseDnsResolver() = default;

  /// Resolve the PTR record for the given address.
  auto resolve(ip address) -> Task<ReverseDnsResult>;

  /// Return a cached PTR result if available and still fresh.
  auto cached(ip address) -> Task<Option<ReverseDnsResult>>;

private:
  using lru_type = std::list<ip>;

  struct CacheEntry {
    ReverseDnsResult result;
    std::chrono::steady_clock::time_point expires_at;
    lru_type::iterator lru_position;
  };

  struct State {
    lru_type lru;
    std::unordered_map<ip, CacheEntry> cache;
  };

  static auto ttl_for(ReverseDnsConfig const& config,
                      ReverseDnsResult const& result)
    -> std::chrono::steady_clock::duration;

  static auto touch(State& state,
                    std::unordered_map<ip, CacheEntry>::iterator it) -> void;

  static auto erase(State& state,
                    std::unordered_map<ip, CacheEntry>::iterator it) -> void;

  static auto evict(State& state, size_t max_entries) -> void;

  ReverseDnsConfig config_;
  Semaphore permits_;
  Mutex<State> state_;
};

} // namespace tenzir
