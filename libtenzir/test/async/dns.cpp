//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/dns.hpp"

#include "tenzir/test/test.hpp"

#include <folly/coro/BlockingWait.h>

#include <array>

namespace tenzir {

namespace {

auto loopback_ip(uint8_t last_octet) -> ip {
  auto bytes = std::array<uint8_t, 4>{127, 0, 0, last_octet};
  return ip::v4(std::span{bytes});
}

} // namespace

TEST("forward dns resolves literal addresses without network access") {
  auto resolver = ForwardDnsResolver{ForwardDnsConfig{
    .positive_ttl = std::chrono::hours{1},
    .negative_ttl = std::chrono::hours{1},
    .literal_ttl = std::chrono::hours{1},
  }};
  auto result = folly::coro::blockingWait(resolver.resolve("127.0.0.1"));
  check_eq(result.status, ForwardDnsStatus::resolved);
  check_eq(result.answers.size(), size_t{1});
  check_eq(result.answers[0].address, loopback_ip(1));
  check_eq(result.answers[0].type, std::string{"A"});
  auto cached = folly::coro::blockingWait(resolver.cached("127.0.0.1"));
  check(static_cast<bool>(cached));
}

TEST("reverse dns resolves loopback addresses without network access") {
  auto resolver = ReverseDnsResolver{};
  auto result = folly::coro::blockingWait(resolver.resolve(loopback_ip(1)));
  check_eq(result.status, ReverseDnsStatus::resolved);
  check(static_cast<bool>(result.hostname));
  check_eq(*result.hostname, std::string{"localhost"});
}

TEST("reverse dns cache evicts least recently used entries") {
  auto resolver = ReverseDnsResolver{ReverseDnsConfig{
    .max_entries = 2,
    .positive_ttl = std::chrono::hours{1},
    .negative_ttl = std::chrono::hours{1},
  }};
  auto ip1 = loopback_ip(1);
  auto ip2 = loopback_ip(2);
  auto ip3 = loopback_ip(3);

  std::ignore = folly::coro::blockingWait(resolver.resolve(ip1));
  std::ignore = folly::coro::blockingWait(resolver.resolve(ip2));

  auto cached1 = folly::coro::blockingWait(resolver.cached(ip1));
  check(static_cast<bool>(cached1));

  std::ignore = folly::coro::blockingWait(resolver.resolve(ip3));

  check(static_cast<bool>(folly::coro::blockingWait(resolver.cached(ip1))));
  check(not static_cast<bool>(folly::coro::blockingWait(resolver.cached(ip2))));
  check(static_cast<bool>(folly::coro::blockingWait(resolver.cached(ip3))));
}

} // namespace tenzir
