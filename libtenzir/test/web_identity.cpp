//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/web_identity.hpp"

#include "tenzir/test/test.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <mutex>
#include <poll.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace tenzir;

namespace {

/// A JWT-shaped string; the token sources never parse it, they only move it.
constexpr auto fake_jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJyZXBvOnRlbnppci9tb2"
                          "5vIn0.c2lnbmF0dXJl";

/// A single-threaded HTTP/1.1 server that answers every request from a
/// callback and records what it received.
class scoped_http_server {
public:
  using handler = std::function<std::string(std::string const& request)>;

  explicit scoped_http_server(handler h) : handler_{std::move(h)} {
    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    REQUIRE(fd_ >= 0);
    auto reuse = 1;
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    auto addr = sockaddr_in{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    REQUIRE(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    auto len = socklen_t{sizeof(addr)};
    REQUIRE(::getsockname(fd_, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    port_ = ::ntohs(addr.sin_port);
    REQUIRE(::listen(fd_, 8) == 0);
    thread_ = std::thread{[this] {
      serve();
    }};
  }

  ~scoped_http_server() {
    stop_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
    ::close(fd_);
  }

  auto url(std::string_view path) const -> std::string {
    return fmt::format("http://127.0.0.1:{}{}", port_, path);
  }

  auto requests() const -> std::vector<std::string> {
    auto lock = std::lock_guard{mutex_};
    return requests_;
  }

private:
  void serve() {
    while (not stop_) {
      auto pfd = pollfd{fd_, POLLIN, 0};
      // Poll instead of blocking in accept(2) so that the destructor's stop
      // flag is observed.
      if (::poll(&pfd, 1, 50) <= 0) {
        continue;
      }
      auto conn = ::accept(fd_, nullptr, nullptr);
      if (conn < 0) {
        continue;
      }
      auto request = std::string{};
      auto buffer = std::array<char, 4096>{};
      while (request.find("\r\n\r\n") == std::string::npos) {
        auto n = ::recv(conn, buffer.data(), buffer.size(), 0);
        if (n <= 0) {
          break;
        }
        request.append(buffer.data(), static_cast<size_t>(n));
      }
      {
        auto lock = std::lock_guard{mutex_};
        requests_.push_back(request);
      }
      auto response = handler_(request);
      auto sent = size_t{0};
      while (sent < response.size()) {
        auto n
          = ::send(conn, response.data() + sent, response.size() - sent, 0);
        if (n <= 0) {
          break;
        }
        sent += static_cast<size_t>(n);
      }
      ::shutdown(conn, SHUT_WR);
      ::close(conn);
    }
  }

  handler handler_;
  int fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<bool> stop_ = false;
  std::thread thread_;
  mutable std::mutex mutex_;
  std::vector<std::string> requests_;
};

auto http_response(std::string_view status, std::string_view body,
                   std::string_view content_type = "application/json")
  -> std::string {
  return fmt::format("HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: "
                     "{}\r\nConnection: close\r\n\r\n{}",
                     status, content_type, body.size(), body);
}

auto endpoint(std::string url,
              std::vector<std::pair<std::string, std::string>> headers,
              Option<std::string> path) -> resolved_web_identity {
  return resolved_token_endpoint{
    .url = std::move(url),
    .headers = std::move(headers),
    .query_params = {},
    .path = std::move(path),
  };
}

} // namespace

TEST("direct token is returned verbatim") {
  auto config = resolved_web_identity{resolved_token{fake_jwt}};
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
}

TEST("token file contents are trimmed") {
  auto path
    = std::filesystem::temp_directory_path() / "tenzir-web-identity-token.jwt";
  {
    auto out = std::ofstream{path};
    out << "  " << fake_jwt << "\n\n";
  }
  auto config = resolved_web_identity{resolved_token_file{path.string()}};
  auto token = fetch_web_identity_token(config);
  std::filesystem::remove(path);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
}

TEST("token endpoint extracts a JSON path") {
  // Mirrors the GitHub Actions OIDC endpoint, which answers `{"value": ...}`.
  auto server = scoped_http_server{[](std::string const& request) {
    if (request.find("Authorization: Bearer request-token")
        == std::string::npos) {
      return http_response("401 Unauthorized", R"({"error":"unauthorized"})");
    }
    return http_response("200 OK",
                         fmt::format(R"({{"value":"{}"}})", fake_jwt));
  }};
  auto config
    = endpoint(server.url("/token?audience=api://AzureADTokenExchange"),
               {{"Authorization", "Bearer request-token"}}, "value");
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
  auto requests = server.requests();
  REQUIRE_EQUAL(requests.size(), 1ull);
  CHECK(requests[0].starts_with("GET /token?audience=api://"
                                "AzureADTokenExchange"));
}

TEST("query parameters are appended and percent-encoded") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK",
                         fmt::format(R"({{"value":"{}"}})", fake_jwt));
  }};
  auto config = resolved_token_endpoint{
    .url = server.url("/token"),
    .headers = {},
    .query_params = {{"audience", "api://AzureADTokenExchange"}},
    .path = "value",
  };
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
  auto requests = server.requests();
  REQUIRE_EQUAL(requests.size(), 1ull);
  CHECK(requests[0].starts_with(
    "GET /token?audience=api%3A%2F%2FAzureADTokenExchange"));
}

TEST("query parameters join a URL that already has a query") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK", fake_jwt, "text/plain");
  }};
  auto config = resolved_token_endpoint{
    .url = server.url("/token?api-version=2.0"),
    .headers = {},
    .query_params = {{"audience", "sts.amazonaws.com"}, {"extra", "1"}},
    .path = None{},
  };
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
  auto requests = server.requests();
  REQUIRE_EQUAL(requests.size(), 1ull);
  CHECK(requests[0].starts_with("GET /token?api-version=2.0"
                                "&audience=sts.amazonaws.com&extra=1"));
}

TEST("token endpoint accepts a leading dot in the JSON path") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK",
                         fmt::format(R"({{"access_token":"{}"}})", fake_jwt));
  }};
  auto config = endpoint(server.url("/token"), {}, ".access_token");
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
}

TEST("a null JSON path yields the plain text body") {
  // Mirrors the GCP metadata server, which answers with a bare JWT.
  auto server = scoped_http_server{[](std::string const& request) {
    if (request.find("Metadata-Flavor: Google") == std::string::npos) {
      return http_response("403 Forbidden", R"({"error":"forbidden"})");
    }
    return http_response("200 OK", fmt::format("{}\n", fake_jwt), "text/plain");
  }};
  auto config = endpoint(server.url("/identity"),
                         {{"Metadata-Flavor", "Google"}}, None{});
  auto token = fetch_web_identity_token(config);
  REQUIRE_NOERROR(token);
  CHECK_EQUAL(*token, fake_jwt);
}

TEST("a missing header fails the request") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("401 Unauthorized", R"({"error":"unauthorized"})");
  }};
  auto config = endpoint(server.url("/token"), {}, "value");
  CHECK_ERROR(fetch_web_identity_token(config));
}

TEST("a JSON path that is absent from the response fails") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK", R"({"value":"x"})");
  }};
  auto config = endpoint(server.url("/token"), {}, "access_token");
  CHECK_ERROR(fetch_web_identity_token(config));
}

TEST("a non-JSON response fails when a JSON path is set") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK", "not json");
  }};
  auto config = endpoint(server.url("/token"), {}, "value");
  CHECK_ERROR(fetch_web_identity_token(config));
}

TEST("a JSON path with simdjson operators is rejected") {
  auto server = scoped_http_server{[](std::string const&) {
    return http_response("200 OK", R"({"value":"x"})");
  }};
  auto config = endpoint(server.url("/token"), {}, "data/token");
  CHECK_ERROR(fetch_web_identity_token(config));
}

TEST("an empty direct token fails") {
  CHECK_ERROR(
    fetch_web_identity_token(resolved_web_identity{resolved_token{}}));
}
