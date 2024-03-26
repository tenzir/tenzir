//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ip.hpp"
#include "tenzir/port.hpp"

#include <arpa/inet.h>
#include <caf/expected.hpp>
#include <caf/uri.hpp>
#include <sys/socket.h>
#include <sys/types.h>

namespace tenzir {

/// The type of a socket.
enum class socket_type {
  invalid,
  tcp,
  udp,
};

/// Wrapper to facilitate interacting with socket addresses.
struct socket_endpoint {
  /// Parses a URL-like string into a socket endpoint, e.g.,
  /// "tcp://localhost:42" or "udp://1.2.3.4"
  static auto parse(std::string_view url) -> caf::expected<socket_endpoint>;

  socket_endpoint() = default;

  auto as_sock_addr() -> sockaddr*;
  auto sock_addr_len() const -> socklen_t;

  socket_type type = {};
  ip addr = {};
  uint16_t port = {};
  std::variant<sockaddr_in, sockaddr_in6> sock_addr;
};

/// RAII wrapper around a plain socket.
struct socket {
  socket() = default;

  socket(ip::family family, socket_type type);

  explicit socket(socket_endpoint endpoint);

  ~socket();

  explicit operator bool() const;

  auto connect(socket_endpoint peer) -> int;

  auto bind(socket_endpoint endpoint) -> int;

  auto recv(std::span<std::byte> buffer, int flags = 0) -> ssize_t;

  auto recvfrom(std::span<std::byte> buffer, socket_endpoint& endpoint,
                int flags = 0) -> ssize_t;

  int fd = -1;
};

/// Performs DNS resolution of a given hostname.
/// @param hostname The hostname to resolve.
/// @returns The list of resolved IP addresses.
auto resolve(std::string_view hostname) -> caf::expected<std::vector<ip>>;

// Conversion utilities.
auto convert(const ip& in, sockaddr_in& out) -> caf::error;
auto convert(const ip& in, sockaddr_in6& out) -> caf::error;

auto convert(const sockaddr_in& in, ip& out) -> caf::error;
auto convert(const sockaddr_in6& in, ip& out) -> caf::error;

} // namespace tenzir
