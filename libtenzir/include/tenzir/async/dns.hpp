//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/variant.hpp"

#include <chrono>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace folly {
class SocketAddress;
} // namespace folly

namespace tenzir {

class ForwardDnsResolver;
class ReverseDnsResolver;

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

/// DNS lookup failed.
struct DnsError {
  std::string error;
};

/// DNS lookup completed successfully but no matching record exists.
struct DnsNotFound {};

/// A single forward DNS answer.
struct ForwardDnsAnswer {
  ip address;
  std::string type;
  std::chrono::seconds ttl{};
};

/// Successful forward DNS lookup.
struct ForwardDnsResolved {
  std::vector<ForwardDnsAnswer> answers;
  Option<std::string> canonical_name = None{};
};

/// Successful reverse DNS lookup.
struct ReverseDnsResolved {
  std::string hostname;
};

template <class Resolved>
using DnsLookup = variant<DnsNotFound, Resolved>;

using ForwardDnsLookup = DnsLookup<ForwardDnsResolved>;
using ReverseDnsLookup = DnsLookup<ReverseDnsResolved>;

/// Result of a forward DNS lookup.
using ForwardDnsResult = Result<ForwardDnsLookup, DnsError>;

/// Result of a reverse DNS lookup.
using ReverseDnsResult = Result<ReverseDnsLookup, DnsError>;

/// The parsed host and port components of a socket address string.
struct ParsedSocketAddress {
  ParsedSocketAddress(std::string host, uint16_t port)
    : host{std::move(host)}, port{port} {
  }

  std::string host;
  uint16_t port;
};

/// The socket address form that a parser should accept.
enum class SocketAddressKind {
  remote,
  bind,
};

/// Socket address parsing succeeded, but resolving the host failed.
struct ResolveAddressError : variant<DnsError, DnsNotFound> {
  using variant<DnsError, DnsNotFound>::variant;
};

/// Parse a socket address string of the form `<host>:<port>`.
///
/// Bind addresses additionally accept `:<port>` when `kind` is `bind`.
auto parse_socket_address(std::string_view endpoint, SocketAddressKind kind)
  -> Option<ParsedSocketAddress>;

namespace detail {

/// Test-only access for injecting deterministic DNS query implementations.
struct DnsResolverTestAccess {
  static auto
  make_forward(ForwardDnsConfig config,
               std::function<Task<ForwardDnsResult>(std::string)> query)
    -> ForwardDnsResolver;

  static auto make_reverse(ReverseDnsConfig config,
                           std::function<Task<ReverseDnsResult>(ip)> query)
    -> ReverseDnsResolver;
};

} // namespace detail

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
  auto resolve(std::string hostname) -> Task<Arc<ForwardDnsResult>>;

  /// Return a cached result if available and still fresh.
  auto cached(std::string_view hostname) -> Task<Option<ForwardDnsResult>>;

  /// Return the resolver startup error when c-ares initialization failed.
  [[nodiscard]] auto startup_error() const -> Option<DnsError>;

  /// Resolve a previously parsed remote socket address.
  auto resolve_socket_address(ParsedSocketAddress endpoint)
    -> Task<Result<folly::SocketAddress, ResolveAddressError>>;

  /// Resolve a previously parsed bind address.
  auto resolve_bind_address(ParsedSocketAddress endpoint)
    -> Task<Result<folly::SocketAddress, ResolveAddressError>>;

private:
  friend struct detail::DnsResolverTestAccess;

  ForwardDnsResolver(ForwardDnsConfig config,
                     std::function<Task<ForwardDnsResult>(std::string)> query);

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
  auto resolve(ip address) -> Task<Arc<ReverseDnsResult>>;

  /// Return a cached result if available and still fresh.
  auto cached(ip address) -> Task<Option<ReverseDnsResult>>;

  /// Return the resolver startup error when c-ares initialization failed.
  [[nodiscard]] auto startup_error() const -> Option<DnsError>;

private:
  friend struct detail::DnsResolverTestAccess;

  ReverseDnsResolver(ReverseDnsConfig config,
                     std::function<Task<ReverseDnsResult>(ip)> query);

  struct Impl;
  Box<Impl> impl_;
};

inline auto detail::DnsResolverTestAccess::make_forward(
  ForwardDnsConfig config,
  std::function<Task<ForwardDnsResult>(std::string)> query)
  -> ForwardDnsResolver {
  return ForwardDnsResolver{std::move(config), std::move(query)};
}

inline auto detail::DnsResolverTestAccess::make_reverse(
  ReverseDnsConfig config, std::function<Task<ReverseDnsResult>(ip)> query)
  -> ReverseDnsResolver {
  return ReverseDnsResolver{std::move(config), std::move(query)};
}

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::DnsError> : formatter<std::string_view> {
  auto format(tenzir::DnsError const& x, format_context& ctx) const
    -> format_context::iterator {
    return formatter<std::string_view>::format(x.error, ctx);
  }
};

template <>
struct formatter<tenzir::DnsNotFound> : formatter<std::string_view> {
  auto format(tenzir::DnsNotFound, format_context& ctx) const
    -> format_context::iterator {
    return formatter<std::string_view>::format("no matching A or AAAA records",
                                               ctx);
  }
};

template <>
struct formatter<tenzir::ResolveAddressError> : formatter<std::string_view> {
  auto format(tenzir::ResolveAddressError const& x, format_context& ctx) const
    -> format_context::iterator {
    return tenzir::match(
      x,
      [&](tenzir::DnsError const& error) {
        return formatter<tenzir::DnsError>{}.format(error, ctx);
      },
      [&](tenzir::DnsNotFound not_found) {
        return formatter<tenzir::DnsNotFound>{}.format(not_found, ctx);
      });
  }
};

} // namespace fmt
