//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/grpc.hpp"

#include "tenzir/test/test.hpp"

using namespace tenzir;

TEST("parse gRPC peer IP") {
  auto ipv4 = detail::grpc_peer_ip("ipv4:192.0.2.1:4317");
  REQUIRE(ipv4.is_ok());
  CHECK_EQUAL(ipv4.unwrap(), "192.0.2.1");
  auto ipv6 = detail::grpc_peer_ip("ipv6:[2001:db8::1]:4317");
  REQUIRE(ipv6.is_ok());
  CHECK_EQUAL(ipv6.unwrap(), "2001:db8::1");
  // gRPC percent-encodes the brackets in the peer URI it hands us, so this is
  // the form the runtime actually produces.
  auto encoded = detail::grpc_peer_ip("ipv6:%5B::1%5D:4317");
  REQUIRE(encoded.is_ok());
  CHECK_EQUAL(encoded.unwrap(), "::1");
  auto encoded_full = detail::grpc_peer_ip("ipv6:%5B2001:db8::1%5D:37368");
  REQUIRE(encoded_full.is_ok());
  CHECK_EQUAL(encoded_full.unwrap(), "2001:db8::1");
  auto encoded_lower = detail::grpc_peer_ip("ipv6:%5b2001:db8::1%5d:37368");
  REQUIRE(encoded_lower.is_ok());
  CHECK_EQUAL(encoded_lower.unwrap(), "2001:db8::1");
  auto invalid_ipv6 = detail::grpc_peer_ip("ipv6:not-an-address");
  REQUIRE(invalid_ipv6.is_err());
  CHECK_EQUAL(invalid_ipv6.unwrap_err(),
              "unsupported gRPC peer address `ipv6:not-an-address`");
  auto unix_peer = detail::grpc_peer_ip("unix:/tmp/otlp.sock");
  REQUIRE(unix_peer.is_err());
  CHECK_EQUAL(unix_peer.unwrap_err(),
              "unsupported gRPC peer address `unix:/tmp/otlp.sock`");
}
