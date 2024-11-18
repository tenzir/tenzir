//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/socket.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/detail/byteswap.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/die.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <arrow/status.h>
#include <arrow/util/uri.h>

#include <netdb.h>

namespace tenzir {

auto socket_endpoint::parse(std::string_view url)
  -> caf::expected<socket_endpoint> {
  auto uri = arrow::util::Uri{};
  if (not uri.Parse(std::string{url}).ok()) {
    return ec::parse_error;
  }
  auto result = socket_endpoint{};
  if (uri.scheme() == "udp") {
    result.type = socket_type::udp;
  } else if (uri.scheme() == "tcp") {
    result.type = socket_type::tcp;
  } else {
    return caf::make_error(ec::parse_error, "invalid URL scheme");
  }
  // Parse port.
  if (uri.port() > 0) {
    result.port = detail::narrow_cast<uint16_t>(uri.port());
  } else {
    return caf::make_error(ec::parse_error, "missing port");
  }
  // Resolve hostname.
  auto ips = resolve(uri.host());
  if (not ips) {
    return ips.error();
  }
  if (ips->empty()) {
    return caf::make_error(ec::parse_error,
                           "host does not resolve to any address");
  }
  // Pick first v4 address.
  auto v4 = std::ranges::find_if(*ips, [](const ip& addr) {
    return addr.is_v4();
  });
  auto v6 = std::ranges::find_if(*ips, [](const ip& addr) {
    return addr.is_v6();
  });
  TENZIR_ASSERT(v4 != ips->end() || v6 != ips->end());
  if (v4 != ips->end()) {
    TENZIR_DEBUG("selecting first IPv4 address: {}", *v4);
    result.addr = *v4;
  } else {
    TENZIR_DEBUG("selecting first IPv6 address: {}", *v6);
    result.addr = *v6;
  }
  // Populate sockaddr variant.
  if (result.addr.is_v4()) {
    auto sa = sockaddr_in{};
    auto err = convert(result.addr, sa);
    TENZIR_ASSERT(not err);
    sa.sin_port = detail::to_network_order(result.port);
    result.sock_addr = sa;
  } else {
    auto sa = sockaddr_in6{};
    auto err = convert(result.addr, sa);
    TENZIR_ASSERT(not err);
    sa.sin6_port = detail::to_network_order(result.port);
    result.sock_addr = sa;
  }
  return result;
}

auto socket_endpoint::as_sock_addr() -> sockaddr* {
  auto cast = detail::overload{
    [](sockaddr_in& sa) {
      return reinterpret_cast<sockaddr*>(&sa);
    },
    [](sockaddr_in6& sa) {
      return reinterpret_cast<sockaddr*>(&sa);
    },
  };
  return std::visit(cast, sock_addr);
}

auto socket_endpoint::sock_addr_len() const -> socklen_t {
  auto size = detail::overload{
    [](const sockaddr_in&) {
      return sizeof(sockaddr_in);
    },
    [](const sockaddr_in6&) {
      return sizeof(sockaddr_in6);
    },
  };
  return std::visit(size, sock_addr);
}

socket::socket() : fd{std::make_unique<int>(-1)} {
}

socket::socket(socket_endpoint endpoint) : socket{} {
  auto domain = endpoint.addr.is_v4() ? AF_INET : AF_INET6;
  switch (endpoint.type) {
    case socket_type::invalid:
      *fd = -1;
      break;
    case socket_type::udp:
      *fd = ::socket(domain, SOCK_DGRAM, 0);
      break;
    case socket_type::tcp:
      *fd = ::socket(domain, SOCK_STREAM, 0);
      break;
  }
}

socket::~socket() {
  if (*this) {
    ::close(*fd);
  }
}

socket::operator bool() const {
  return fd and *fd >= 0;
}

auto socket::connect(socket_endpoint peer) -> int {
  return ::connect(*fd, peer.as_sock_addr(), peer.sock_addr_len());
}

auto socket::bind(socket_endpoint local) -> int {
  return ::bind(*fd, local.as_sock_addr(), local.sock_addr_len());
}

auto socket::recv(std::span<std::byte> buffer, int flags) -> ssize_t {
  return ::recv(*fd, buffer.data(), buffer.size(), flags);
}

auto socket::recvfrom(std::span<std::byte> buffer, socket_endpoint& endpoint,
                      int flags) -> ssize_t {
  auto sock_addr_len = endpoint.sock_addr_len();
  return ::recvfrom(*fd, buffer.data(), buffer.size(), flags,
                    endpoint.as_sock_addr(), &sock_addr_len);
}

auto socket::send(std::span<const std::byte> buffer, int flags) -> ssize_t {
  return ::send(*fd, buffer.data(), buffer.size(), flags);
}

auto socket::sendto(std::span<const std::byte> buffer,
                    socket_endpoint& endpoint, int flags) -> ssize_t {
  return ::sendto(*fd, buffer.data(), buffer.size(), flags,
                  endpoint.as_sock_addr(), endpoint.sock_addr_len());
}

auto resolve(std::string_view hostname) -> caf::expected<std::vector<ip>> {
  addrinfo hints = {};
  std::memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  addrinfo* res = nullptr;
  TENZIR_DEBUG("resolving {}", hostname);
  auto status = getaddrinfo(hostname.data(), nullptr, &hints, &res);
  if (status != 0) {
    return caf::make_error(ec::parse_error, gai_strerror(status));
  }
  auto result = std::vector<ip>{};
  for (auto* ptr = res; ptr != nullptr; ptr = ptr->ai_next) {
    if (ptr->ai_family == AF_INET) {
      auto* ptr_v4 = reinterpret_cast<sockaddr_in*>(ptr->ai_addr);
      auto addr = to<ip>(*ptr_v4);
      TENZIR_ASSERT(addr);
      result.push_back(*addr);
    } else if (ptr->ai_family == AF_INET6) {
      auto* ptr_v6 = reinterpret_cast<sockaddr_in6*>(ptr->ai_addr);
      auto addr = to<ip>(*ptr_v6);
      TENZIR_ASSERT(addr);
      result.push_back(*addr);
    } else {
      die("unsupported IP address family");
    }
  }
  freeaddrinfo(res);
  return result;
}

auto convert(const ip& in, sockaddr_in& out) -> caf::error {
  TENZIR_ASSERT(in.is_v4());
  auto bytes = as_bytes(in).subspan(12);
  TENZIR_ASSERT(bytes.size() == 4);
  std::memset(&out, 0, sizeof(out));
  out.sin_family = AF_INET;
  std::memcpy(&out.sin_addr, bytes.data(), 4);
  return {};
}

auto convert(const ip& in, sockaddr_in6& out) -> caf::error {
  TENZIR_ASSERT(in.is_v6());
  std::memset(&out, 0, sizeof(out));
  out.sin6_family = AF_INET6;
  auto bytes = as_bytes(in);
  std::memcpy(&out.sin6_addr, bytes.data(), bytes.size());
  return {};
}

auto convert(const sockaddr_in& in, ip& out) -> caf::error {
  out = ip::v4(detail::to_host_order(in.sin_addr.s_addr));
  return {};
}

auto convert(const sockaddr_in6& in, ip& out) -> caf::error {
  out = ip::v6(as_bytes<16>(&in.sin6_addr, 16));
  return {};
}

} // namespace tenzir
