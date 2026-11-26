// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/controller/http_transport.hpp"

#include "tenzir/test/test.hpp"

#include <arpa/inet.h>
#include <folly/coro/BlockingWait.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
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

  /// Takes one connection, reads a request whose body is `{}`, and answers it
  /// with `response` as written.
  auto answer(std::string_view response) const -> void {
    auto connection = ::accept(fd_, nullptr, nullptr);
    if (connection < 0) {
      return;
    }
    auto request = std::string{};
    auto buffer = std::array<char, 1024>{};
    while (not request.ends_with("\r\n\r\n{}")) {
      auto read = ::read(connection, buffer.data(), buffer.size());
      if (read <= 0) {
        break;
      }
      request.append(buffer.data(), static_cast<std::size_t>(read));
    }
    std::ignore = ::write(connection, response.data(), response.size());
    ::close(connection);
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

TEST("HTTP transport passes on how long the peer asks to be left alone") {
  auto socket = LoopbackSocket{true};
  // Lowercase, as over HTTP/2.
  auto server = std::thread{[&] {
    socket.answer("HTTP/1.1 429 Too Many Requests\r\n"
                  "retry-after: 7\r\n"
                  "content-length: 0\r\n"
                  "connection: close\r\n\r\n");
  }};
  auto result = attempt(socket.url(), std::chrono::seconds{2});
  server.join();
  REQUIRE(result.is_ok());
  check(result.unwrap().status == 429);
  REQUIRE(result.unwrap().retry_after);
  check(*result.unwrap().retry_after == std::chrono::seconds{7});
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
