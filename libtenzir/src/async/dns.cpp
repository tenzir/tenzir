//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/dns.hpp"

#include "tenzir/async/blocking_executor.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/socket.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace tenzir {

namespace {

auto reverse_dns_lookup(ip address) -> ReverseDnsResult {
  auto storage = sockaddr_storage{};
  auto length = socklen_t{};
  if (address.is_v4()) {
    auto sockaddr = sockaddr_in{};
    auto err = convert(address, sockaddr);
    TENZIR_ASSERT(not err);
    std::memcpy(&storage, &sockaddr, sizeof(sockaddr));
    length = sizeof(sockaddr);
  } else {
    auto sockaddr = sockaddr_in6{};
    auto err = convert(address, sockaddr);
    TENZIR_ASSERT(not err);
    std::memcpy(&storage, &sockaddr, sizeof(sockaddr));
    length = sizeof(sockaddr);
  }
  auto host = std::array<char, NI_MAXHOST>{};
  auto status = ::getnameinfo(reinterpret_cast<sockaddr const*>(&storage),
                              length, host.data(), host.size(), nullptr, 0,
                              NI_NAMEREQD);
  if (status == 0) {
    return {
      .status = ReverseDnsStatus::resolved,
      .hostname = std::string{host.data()},
    };
  }
  auto is_not_found = status == EAI_NONAME;
#if defined(EAI_NODATA) && EAI_NODATA != EAI_NONAME
  is_not_found = is_not_found or status == EAI_NODATA;
#endif
  if (is_not_found) {
    return {.status = ReverseDnsStatus::not_found};
  }
  return {
    .status = ReverseDnsStatus::failed,
    .error = std::string{::gai_strerror(status)},
  };
}

} // namespace

ReverseDnsResolver::ReverseDnsResolver(ReverseDnsConfig config)
  : config_{std::move(config)},
    permits_{std::max(size_t{1}, config_.max_in_flight)},
    state_{State{}} {
}

auto ReverseDnsResolver::resolve(ip address) -> Task<ReverseDnsResult> {
  if (auto result = co_await cached(address)) {
    co_return std::move(*result);
  }
  auto permit = co_await permits_.acquire();
  if (auto result = co_await cached(address)) {
    co_return std::move(*result);
  }
  auto result = co_await spawn_blocking([address] {
    return reverse_dns_lookup(address);
  });
  auto ttl = ttl_for(config_, result);
  if (ttl > std::chrono::steady_clock::duration::zero()) {
    auto state = co_await state_.lock();
    state->cache.insert_or_assign(
      address, CacheEntry{.result = result,
                          .expires_at = std::chrono::steady_clock::now() + ttl});
  }
  co_return result;
}

auto ReverseDnsResolver::cached(ip address)
  -> Task<Option<ReverseDnsResult>> {
  auto state = co_await state_.lock();
  auto now = std::chrono::steady_clock::now();
  auto it = state->cache.find(address);
  if (it == state->cache.end()) {
    co_return None{};
  }
  if (it->second.expires_at <= now) {
    state->cache.erase(it);
    co_return None{};
  }
  co_return it->second.result;
}

auto ReverseDnsResolver::ttl_for(ReverseDnsConfig const& config,
                                 ReverseDnsResult const& result)
  -> std::chrono::steady_clock::duration {
  switch (result.status) {
    case ReverseDnsStatus::resolved:
      return config.positive_ttl;
    case ReverseDnsStatus::not_found:
    case ReverseDnsStatus::failed:
      return config.negative_ttl;
  }
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
