//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/option.hpp"

#include <chrono>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir {

/// Shared configuration for async DNS resolvers.
struct DnsResolverConfig {
  /// Maximum number of concurrent lookups.
  size_t max_in_flight = 32;

  /// Maximum number of cached results.
  size_t max_entries = 4096;

  /// Timeout for a single c-ares lookup.
  std::chrono::milliseconds timeout{5000};

  /// Number of retry attempts for a single c-ares lookup.
  size_t tries = 2;

  /// Cache duration for successful lookups when the backend provides no TTL.
  std::chrono::seconds positive_ttl{300};

  /// Cache duration for missing records or lookup failures.
  std::chrono::seconds negative_ttl{30};

  /// Synthetic TTL for literal addresses and localhost shortcuts.
  std::chrono::seconds literal_ttl{60};
};

using ForwardDnsConfig = DnsResolverConfig;
using ReverseDnsConfig = DnsResolverConfig;

enum class DnsStatus {
  resolved,
  not_found,
  failed,
};

using ForwardDnsStatus = DnsStatus;
using ReverseDnsStatus = DnsStatus;

/// A single forward DNS answer.
struct ForwardDnsAnswer {
  ip address;
  std::string type;
  std::chrono::seconds ttl{};
};

/// Result of a forward DNS lookup.
struct ForwardDnsResult {
  ForwardDnsStatus status = ForwardDnsStatus::not_found;
  std::vector<ForwardDnsAnswer> answers;
  Option<std::string> canonical_name = None{};
  Option<std::string> error = None{};
};

/// Result of a reverse DNS lookup.
struct ReverseDnsResult {
  ReverseDnsStatus status = ReverseDnsStatus::not_found;
  Option<std::string> hostname = None{};
  Option<std::string> error = None{};
};

/// Async hostname resolver backed by c-ares with caching and bounded
/// concurrency.
class ForwardDnsResolver {
public:
  explicit ForwardDnsResolver(ForwardDnsConfig config = {});

  ForwardDnsResolver(ForwardDnsResolver const&) = delete;
  auto operator=(ForwardDnsResolver const&) -> ForwardDnsResolver& = delete;
  ForwardDnsResolver(ForwardDnsResolver&&) noexcept;
  auto operator=(ForwardDnsResolver&&) noexcept -> ForwardDnsResolver&;
  ~ForwardDnsResolver();

  /// Resolve all A and AAAA records for the given hostname.
  auto resolve(std::string hostname) -> Task<ForwardDnsResult>;

  /// Return a cached result if available and still fresh.
  auto cached(std::string_view hostname) -> Task<Option<ForwardDnsResult>>;

private:
  struct Impl;
  Box<Impl> impl_;
};

/// Async reverse DNS resolver backed by c-ares with caching and bounded
/// concurrency.
class ReverseDnsResolver {
public:
  explicit ReverseDnsResolver(ReverseDnsConfig config = {});

  ReverseDnsResolver(ReverseDnsResolver const&) = delete;
  auto operator=(ReverseDnsResolver const&) -> ReverseDnsResolver& = delete;
  ReverseDnsResolver(ReverseDnsResolver&&) noexcept;
  auto operator=(ReverseDnsResolver&&) noexcept -> ReverseDnsResolver&;
  ~ReverseDnsResolver();

  /// Resolve the PTR record for the given address.
  auto resolve(ip address) -> Task<ReverseDnsResult>;

  /// Return a cached result if available and still fresh.
  auto cached(ip address) -> Task<Option<ReverseDnsResult>>;

private:
  struct Impl;
  Box<Impl> impl_;
};

} // namespace tenzir
