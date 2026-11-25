// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/controller/http_transport.hpp"

#include "tenzir/test/test.hpp"

#include <arpa/inet.h>
#include <folly/coro/BlockingWait.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <unistd.h>

namespace tenzir {
namespace {

/// A TCP socket on a free port of the loopback interface, closed on scope
/// exit. With `listen`, the kernel completes connections to it, but nothing
/// ever reads from them or answers. Without, it refuses them, and holds the
/// port so that nothing else can take it meanwhile.
class LoopbackSocket {
public:
  explicit LoopbackSocket(bool listen)
    : fd_{::socket(AF_INET, SOCK_STREAM, 0)} {
    REQUIRE(fd_ >= 0);
    auto address = sockaddr_in{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    REQUIRE(::bind(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address))
            == 0);
    auto length = socklen_t{sizeof(address)};
    REQUIRE(::getsockname(fd_, reinterpret_cast<sockaddr*>(&address), &length)
            == 0);
    port_ = ntohs(address.sin_port);
    if (listen) {
      REQUIRE(::listen(fd_, 8) == 0);
    }
  }

  ~LoopbackSocket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  LoopbackSocket(LoopbackSocket const&) = delete;
  auto operator=(LoopbackSocket const&) -> LoopbackSocket& = delete;

  auto url() const -> std::string {
    return "http://127.0.0.1:" + std::to_string(port_);
  }

private:
  int fd_;
  uint16_t port_ = 0;
};

auto attempt(std::string const& url, std::chrono::milliseconds timeout)
  -> Result<HttpResponse, RequestError> {
  auto transport = BufferedHttpTransport{url, "key", timeout};
  return folly::coro::blockingWait(
    transport.request(HttpRequest{"POST", "/telemetry", "{}", {}}));
}

} // namespace

TEST("HTTP transport reports a peer that does not answer as a timeout") {
  auto socket = LoopbackSocket{true};
  auto result = attempt(socket.url(), std::chrono::milliseconds{200});
  REQUIRE(result.is_err());
  check(result.unwrap_err().kind == RequestError::Kind::timeout);
}

TEST("HTTP transport reports a refused connection as unreachable") {
  auto socket = LoopbackSocket{false};
  auto result = attempt(socket.url(), std::chrono::seconds{2});
  REQUIRE(result.is_err());
  check(result.unwrap_err().kind == RequestError::Kind::unreachable);
}

TEST("HTTP transport reports a TLS peer that never completes the handshake "
     "as unreachable") {
  // The connection is made, but the platform behind it never answers, as
  // with a stale address or a proxy that cannot reach it.
  auto socket = LoopbackSocket{true};
  auto url = socket.url();
  url.replace(0, 4, "https");
  auto result = attempt(url, std::chrono::milliseconds{300});
  REQUIRE(result.is_err());
  check(result.unwrap_err().kind == RequestError::Kind::unreachable);
}

} // namespace tenzir
