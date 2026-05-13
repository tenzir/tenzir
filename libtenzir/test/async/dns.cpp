//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/dns.hpp"

#include "tenzir/async/notify.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/concept/parseable/tenzir/endpoint.hpp"
#include "tenzir/test/test.hpp"

#include <folly/CancellationToken.h>
#include <folly/OperationCancelled.h>
#include <folly/SocketAddress.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/WithCancellation.h>

#include <array>
#include <future>
#include <thread>

namespace tenzir {

namespace {

auto loopback_ip(uint8_t last_octet) -> ip {
  auto bytes = std::array<uint8_t, 4>{127, 0, 0, last_octet};
  return ip::v4(std::span{bytes});
}

auto loopback_ipv6() -> ip {
  auto bytes = std::array<uint8_t, 16>{
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
  };
  return ip::v6(std::span{bytes});
}

struct DelayedForwardQueryState {
  DelayedForwardQueryState()
    : first_started{first_started_promise.get_future().share()},
      second_started{second_started_promise.get_future().share()} {
  }

  Notify release_first;
  Notify release_second;
  std::promise<void> first_started_promise;
  std::shared_future<void> first_started;
  std::promise<void> second_started_promise;
  std::shared_future<void> second_started;
  Atomic<int> started = 0;
};

} // namespace

TEST("forward dns resolves literal addresses without network access") {
  auto resolver = ForwardDnsResolver{ForwardDnsConfig{
    .positive_ttl = std::chrono::hours{1},
    .negative_ttl = std::chrono::hours{1},
    .literal_ttl = std::chrono::hours{1},
  }};
  auto result = folly::coro::blockingWait(resolver.resolve("127.0.0.1"));
  require(not result->is_err());
  auto* resolved = try_as<ForwardDnsResolved>(&result->unwrap());
  require(static_cast<bool>(resolved));
  check_eq(resolved->answers.size(), size_t{1});
  check_eq(resolved->answers[0].address, loopback_ip(1));
  check_eq(resolved->answers[0].type, std::string{"A"});
  auto cached = folly::coro::blockingWait(resolver.cached("127.0.0.1"));
  check(static_cast<bool>(cached));
  require(not cached->is_err());
  check(static_cast<bool>(try_as<ForwardDnsResolved>(&cached->unwrap())));
}

TEST("forward dns zero ttl answers are not cached") {
  auto resolver = ForwardDnsResolver{ForwardDnsConfig{
    .literal_ttl = std::chrono::seconds{0},
  }};
  auto result = folly::coro::blockingWait(resolver.resolve("127.0.0.1"));
  require(not result->is_err());
  check(folly::coro::blockingWait(resolver.cached("127.0.0.1")) == None{});
}

TEST("forward dns cached ttl reflects remaining lifetime") {
  auto resolver = detail::DnsResolverTestAccess::make_forward(
    ForwardDnsConfig{
      .positive_ttl = std::chrono::hours{1},
      .negative_ttl = std::chrono::hours{1},
      .literal_ttl = std::chrono::hours{1},
    },
    [](std::string) -> Task<ForwardDnsResult> {
      co_return ForwardDnsLookup{ForwardDnsResolved{
        .answers = {{
          .address = loopback_ip(1),
          .type = "A",
          .ttl = std::chrono::seconds{5},
        }},
      }};
    });
  auto result = folly::coro::blockingWait(resolver.resolve("one.example"));
  require(not result->is_err());
  std::this_thread::sleep_for(std::chrono::milliseconds{1100});
  auto cached = folly::coro::blockingWait(resolver.cached("one.example"));
  check(static_cast<bool>(cached));
  require(not cached->is_err());
  auto* resolved = try_as<ForwardDnsResolved>(&cached->unwrap());
  require(static_cast<bool>(resolved));
  check_eq(resolved->answers.size(), size_t{1});
  check(resolved->answers[0].ttl < std::chrono::seconds{5});
  check(resolved->answers[0].ttl > std::chrono::seconds{0});
}

TEST("forward dns resolves remote socket addresses") {
  auto resolver = detail::DnsResolverTestAccess::make_forward(
    ForwardDnsConfig{
      .positive_ttl = std::chrono::hours{1},
      .negative_ttl = std::chrono::hours{1},
      .literal_ttl = std::chrono::hours{1},
    },
    [](std::string) -> Task<ForwardDnsResult> {
      co_return ForwardDnsLookup{ForwardDnsResolved{
        .answers = {{
          .address = loopback_ip(42),
          .type = "A",
          .ttl = std::chrono::seconds{60},
        }},
      }};
    });
  auto endpoint = tenzir::Endpoint{};
  require(parsers::endpoint("dns.test:9000", endpoint));
  auto result = folly::coro::blockingWait(
    resolver.resolve_socket_address(std::move(endpoint)));
  require(not result.is_err());
  check_eq(result.unwrap().describe(), std::string{"127.0.0.42:9000"});
}

TEST("forward dns prefers ipv4 answer over preceding ipv6 answer") {
  auto resolver = detail::DnsResolverTestAccess::make_forward(
    ForwardDnsConfig{
      .positive_ttl = std::chrono::hours{1},
      .negative_ttl = std::chrono::hours{1},
      .literal_ttl = std::chrono::hours{1},
    },
    [](std::string) -> Task<ForwardDnsResult> {
      co_return ForwardDnsLookup{ForwardDnsResolved{
        .answers = {
          {
            .address = loopback_ipv6(),
            .type = "AAAA",
            .ttl = std::chrono::seconds{60},
          },
          {
            .address = loopback_ip(7),
            .type = "A",
            .ttl = std::chrono::seconds{60},
          },
        },
      }};
    });
  auto endpoint = tenzir::Endpoint{};
  require(parsers::endpoint("dual.test:1234", endpoint));
  auto result = folly::coro::blockingWait(
    resolver.resolve_socket_address(std::move(endpoint)));
  require(not result.is_err());
  check_eq(result.unwrap().describe(), std::string{"127.0.0.7:1234"});
}

TEST("forward dns falls back to ipv6 answer when no ipv4 is present") {
  auto resolver = detail::DnsResolverTestAccess::make_forward(
    ForwardDnsConfig{
      .positive_ttl = std::chrono::hours{1},
      .negative_ttl = std::chrono::hours{1},
      .literal_ttl = std::chrono::hours{1},
    },
    [](std::string) -> Task<ForwardDnsResult> {
      co_return ForwardDnsLookup{ForwardDnsResolved{
        .answers = {{
          .address = loopback_ipv6(),
          .type = "AAAA",
          .ttl = std::chrono::seconds{60},
        }},
      }};
    });
  auto endpoint = tenzir::Endpoint{};
  require(parsers::endpoint("v6only.test:5555", endpoint));
  auto result = folly::coro::blockingWait(
    resolver.resolve_socket_address(std::move(endpoint)));
  require(not result.is_err());
  check_eq(result.unwrap().describe(), std::string{"[::1]:5555"});
}

TEST("forward dns resolves bind socket addresses with empty host") {
  auto resolver = ForwardDnsResolver{};
  auto endpoint = tenzir::Endpoint{};
  require(parsers::endpoint(":4242", endpoint));
  auto result = folly::coro::blockingWait(
    resolver.resolve_bind_address(std::move(endpoint)));
  require(not result.is_err());
  check(result.unwrap().getIPAddress().isZero());
  check_eq(result.unwrap().getPort(), uint16_t{4242});
}

TEST("forward dns remote socket addresses require a port") {
  auto endpoint = tenzir::Endpoint{};
  require(parsers::endpoint("missing-port", endpoint));
  check(not endpoint.port);
}

TEST("reverse dns resolves loopback addresses without network access") {
  auto resolver = ReverseDnsResolver{};
  auto result = folly::coro::blockingWait(resolver.resolve(loopback_ip(1)));
  require(not result->is_err());
  auto* resolved = try_as<ReverseDnsResolved>(&result->unwrap());
  require(static_cast<bool>(resolved));
  check_eq(resolved->hostname, std::string{"localhost"});
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

TEST("cancelled forward dns lookups keep their permit until completion") {
  auto state = Arc<DelayedForwardQueryState>{std::in_place};
  auto resolver = detail::DnsResolverTestAccess::make_forward(
    ForwardDnsConfig{
      .max_in_flight = 1,
      .positive_ttl = std::chrono::hours{1},
      .negative_ttl = std::chrono::hours{1},
      .literal_ttl = std::chrono::hours{1},
    },
    [state](std::string) mutable -> Task<ForwardDnsResult> {
      auto index = state->started.fetch_add(1, std::memory_order_relaxed) + 1;
      if (index == 1) {
        state->first_started_promise.set_value();
        co_await state->release_first.wait();
      } else {
        TENZIR_ASSERT(index == 2);
        state->second_started_promise.set_value();
        co_await state->release_second.wait();
      }
      co_return ForwardDnsLookup{DnsNotFound{}};
    });
  auto cancelled = Atomic<bool>{false};
  auto second_ok = Atomic<bool>{false};
  auto cancel = folly::CancellationSource{};
  auto first = std::thread{[&]() {
    try {
      std::ignore = folly::coro::blockingWait(folly::coro::co_withCancellation(
        cancel.getToken(), resolver.resolve("one.example")));
    } catch (folly::OperationCancelled const&) {
      cancelled.store(true, std::memory_order_relaxed);
    }
  }};
  check_eq(state->first_started.wait_for(std::chrono::seconds{1}),
           std::future_status::ready);
  cancel.requestCancellation();
  first.join();
  check(cancelled.load(std::memory_order_relaxed));
  auto second = std::thread{[&]() {
    auto result = folly::coro::blockingWait(resolver.resolve("two.example"));
    second_ok.store(not result->is_err() and is<DnsNotFound>(result->unwrap()),
                    std::memory_order_relaxed);
  }};
  check_eq(state->second_started.wait_for(std::chrono::milliseconds{100}),
           std::future_status::timeout);
  state->release_first.notify_one();
  check_eq(state->second_started.wait_for(std::chrono::seconds{1}),
           std::future_status::ready);
  state->release_second.notify_one();
  second.join();
  check(second_ok.load(std::memory_order_relaxed));
  check_eq(state->started.load(std::memory_order_relaxed), 2);
}

} // namespace tenzir
