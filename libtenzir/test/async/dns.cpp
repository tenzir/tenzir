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

namespace tenzir {

namespace {

auto loopback_ip(uint8_t last_octet) -> ip {
  auto bytes = std::array<uint8_t, 4>{127, 0, 0, last_octet};
  return ip::v4(std::span{bytes});
}

} // namespace

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
  check(not static_cast<bool>(
    folly::coro::blockingWait(resolver.cached(ip2))));
  check(static_cast<bool>(folly::coro::blockingWait(resolver.cached(ip3))));
}

} // namespace tenzir
