//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/http.hpp"

#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/http.hpp"
#include "tenzir/http_pool.hpp"
#include "tenzir/test/test.hpp"

#include <arpa/inet.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <chrono>
#include <map>
#include <span>
#include <string_view>
#include <thread>
#include <unistd.h>

using namespace tenzir;
using namespace std::string_literals;

namespace {

auto make_item = [](std::string_view str) {
  auto result = http::RequestItem::parse(str);
  REQUIRE(result);
  return *result;
};

} // namespace

TEST("parse HTTP request item") {
  auto separators = std::array{":=@", ":=", "==", "=@", "@", "=", ":"};
  auto types = std::array{
    http::RequestItem::file_data_json, http::RequestItem::data_json,
    http::RequestItem::url_param,      http::RequestItem::file_data,
    http::RequestItem::file_form,      http::RequestItem::data,
    http::RequestItem::header,
  };
  static_assert(separators.size() == types.size());
  for (auto i = 0u; i < types.size(); ++i) {
    auto sep = separators[i];
    auto str = fmt::format("foo{}bar", sep);
    auto item = http::RequestItem::parse(str);
    REQUIRE(item);
    CHECK_EQUAL(item->key, "foo");
    CHECK_EQUAL(item->value, "bar");
    CHECK_EQUAL(item->type, types[i]);
    str = fmt::format("foo{}bar\\{}", sep, sep);
    item = http::RequestItem::parse(str);
    REQUIRE(item);
    CHECK_EQUAL(item->key, "foo");
    auto value = fmt::format("bar\\{}", sep);
    CHECK_EQUAL(item->value, value);
  }
}

TEST("HTTP request items - JSON") {
  auto request = http::Request{};
  auto items = std::vector<http::RequestItem>{
    make_item("Content-Type:application/json"),
    make_item("foo:=42"),
  };
  auto err = apply(items, request);
  REQUIRE_EQUAL(err, caf::none);
  // If we have a Content-Type header, apply also adds an Accept header. So here
  // we have 1 explicit header from the request item, plus one implicit Accept
  // header.
  CHECK_EQUAL(request.headers.size(), 1ull + 1);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json, */*");
  // Adding an item with (JSON) data makes the method POST.
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "{\"foo\":42}");
}

TEST("HTTP request items - JSON without content type") {
  auto request = http::Request{};
  auto items = std::vector<http::RequestItem>{
    make_item("foo:=42"),
  };
  auto err = apply(items, request);
  REQUIRE_EQUAL(err, caf::none);
  CHECK_EQUAL(request.headers.size(), 2ull);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json, */*");
  header = request.header("Content-Type");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "application/json");
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "{\"foo\":42}");
}

TEST("HTTP request items - urlencoded") {
  auto request = http::Request{};
  auto items = std::vector<http::RequestItem>{
    make_item("Content-Type:application/x-www-form-urlencoded"),
    make_item("foo:=42"),
    make_item("bar:=true"),
  };
  auto err = apply(items, request);
  CHECK_EQUAL(request.headers.size(), 1ull + 1);
  const auto* header = request.header("Accept");
  REQUIRE(header);
  CHECK_EQUAL(header->value, "*/*");
  REQUIRE_EQUAL(err, caf::none);
  CHECK_EQUAL(request.method, "POST");
  CHECK_EQUAL(request.body, "foo=42&bar=true");
}

TEST("HTTP request items - URL param") {
  auto request = http::Request{};
  auto items = std::vector<http::RequestItem>{
    make_item("foo==42"),
    make_item("bar==true"),
  };
  request.uri = "https://example.org/";
  auto err = apply(items, request);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(request.uri, "https://example.org/?foo=42&bar=true");
  request.uri = "https://example.org/?";
  err = apply(items, request);
  CHECK_EQUAL(err, caf::none);
  CHECK_EQUAL(request.uri, "https://example.org/?foo=42&bar=true");
}

TEST("HTTP header") {
  auto p = make_parser<http::Header>();
  auto str = "foo: bar"s;
  auto f = str.begin();
  auto l = str.end();
  http::Header hdr;
  CHECK(p(f, l, hdr));
  CHECK(hdr.name == "FOO");
  CHECK(hdr.value == "bar");
  CHECK(f == l);
  str = "Content-Type:application/pdf";
  f = str.begin();
  l = str.end();
  CHECK(p(f, l, hdr));
  CHECK(hdr.name == "CONTENT-TYPE");
  CHECK(hdr.value == "application/pdf");
  CHECK(f == l);
}

TEST("parse HTTP Retry-After numeric bounds") {
  for (auto expected : {std::chrono::seconds{0}, std::chrono::seconds{20},
                        std::chrono::seconds::max()}) {
    auto parsed = http::parse_retry_after(fmt::to_string(expected.count()));
    REQUIRE(parsed);
    CHECK_EQUAL(parsed->count(), expected.count());
  }
  for (auto value : {"", "-1", "-0", "1.5", "invalid", "9223372036854775808",
                     "18446744073709551615", "18446744073709551616"}) {
    CHECK(not http::parse_retry_after(value));
  }
}

TEST("parse HTTP Retry-After past date") {
  auto parsed = http::parse_retry_after("Thu, 01 Jan 1970 00:00:00 GMT");
  REQUIRE(parsed);
  CHECK_EQUAL(parsed->count(), 0);
}

TEST("compress request body") {
  auto dh = null_diagnostic_handler{};
  auto encoded = http::compress_request_body("hello"s, " GZip ", dh);
  CHECK(encoded.content_encoding);
  CHECK_EQUAL(*encoded.content_encoding, "gzip");
  auto headers = std::vector<http::Header>{};
  http::add_request_body_headers(headers, encoded);
  CHECK_EQUAL(*http::find(headers, "Content-Encoding"), "gzip");
  CHECK_EQUAL(*http::find(headers, "Content-Length"),
              fmt::to_string(encoded.body.size()));
  auto decompressor = http::make_decompressor("gzip", dh);
  REQUIRE(decompressor);
  auto input = std::span{
    reinterpret_cast<std::byte const*>(encoded.body.data()),
    encoded.body.size(),
  };
  auto decompressed = http::decompress_chunk(**decompressor, input, dh);
  REQUIRE(decompressed);
  auto body = std::move(decompressed).unwrap();
  auto result = std::string{
    reinterpret_cast<char const*>(body.data()),
    body.size(),
  };
  CHECK_EQUAL(result, "hello");
}

TEST("decompress gzip trailer after output limit") {
  auto dh = null_diagnostic_handler{};
  auto encoded = http::compress_request_body("hello"s, "gzip", dh);
  auto decompressor = http::make_decompressor("gzip", dh);
  REQUIRE(decompressor);
  REQUIRE(encoded.body.size() > 8);
  auto const bytes = std::span{
    reinterpret_cast<std::byte const*>(encoded.body.data()),
    encoded.body.size(),
  };
  auto data = http::decompress_chunk_with_status(
    **decompressor, bytes.first(bytes.size() - 8), dh, size_t{5});
  REQUIRE(data);
  auto decompressed = std::move(data).unwrap();
  CHECK_EQUAL(decompressed.bytes.size(), 5u);
  CHECK(not decompressed.finished);
  auto trailer = http::decompress_chunk_with_status(
    **decompressor, bytes.last(8), dh, size_t{0});
  REQUIRE(trailer);
  auto finished = std::move(trailer).unwrap();
  CHECK(finished.bytes.empty());
  CHECK(finished.finished);
}

TEST("same URL host") {
  CHECK(http::is_same_host("api.example", "api.example"));
  CHECK(http::is_same_host("API.Example", "api.example"));
  CHECK(http::is_same_host("api.example.", "api.example"));
  CHECK(http::is_same_host("API.example.", "api.example"));
  CHECK(http::is_same_host("127.0.0.1", "127.0.0.1"));
  CHECK(not http::is_same_host("api.example", "other.example"));
  CHECK(not http::is_same_host("api.example", "api.example.org"));
  CHECK(not http::is_same_host("api.example..", "api.example"));
  CHECK(not http::is_same_host("127.0.0.1", "127.0.0.2"));
  // IP literals compare as addresses.
  CHECK(http::is_same_host("[2001:db8::1]", "[2001:0db8:0:0:0:0:0:1]"));
  CHECK(http::is_same_host("[2001:DB8::1]", "2001:db8::1"));
  CHECK(not http::is_same_host("[2001:db8::1]", "[2001:db8::2]"));
  CHECK(not http::is_same_host("127.0.0.1", "localhost"));
  CHECK(not http::is_same_host("[::1]", "localhost"));
}

namespace {

class StreamRetryServer {
public:
  explicit StreamRetryServer(int expected_requests)
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
    REQUIRE(::listen(fd_, 8) == 0);
    worker_ = std::thread{[this, expected_requests] {
      for (auto i = 0; i < expected_requests; ++i) {
        auto conn = ::accept(fd_, nullptr, nullptr);
        if (conn < 0) {
          break;
        }
        auto const attempt = attempts_.fetch_add(1) + 1;
        auto req = std::string{};
        auto buf = std::array<char, 1024>{};
        while (req.find("\r\n\r\n") == std::string::npos) {
          auto n = ::read(conn, buf.data(), buf.size());
          if (n <= 0) {
            break;
          }
          req.append(buf.data(), static_cast<size_t>(n));
        }
        if (attempt == 1) {
          // Send final 200 headers with 0 body bytes and stall past client
          // read timeout before closing.
          constexpr auto hdrs
            = std::string_view{"HTTP/1.1 200 OK\r\nContent-Length: "
                               "10\r\nConnection: close\r\n\r\n"};
          std::ignore = ::write(conn, hdrs.data(), hdrs.size());
          std::this_thread::sleep_for(std::chrono::milliseconds{300});
        } else {
          constexpr auto resp
            = std::string_view{"HTTP/1.1 200 OK\r\nContent-Length: "
                               "2\r\nConnection: close\r\n\r\nok"};
          std::ignore = ::write(conn, resp.data(), resp.size());
        }
        ::close(conn);
      }
    }};
  }

  ~StreamRetryServer() {
    if (fd_ >= 0) {
      ::shutdown(fd_, SHUT_RDWR);
      ::close(fd_);
    }
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  StreamRetryServer(StreamRetryServer const&) = delete;
  auto operator=(StreamRetryServer const&) -> StreamRetryServer& = delete;

  auto url() const -> std::string {
    return fmt::format("http://127.0.0.1:{}/test", port_);
  }

  auto attempts() const -> int {
    return attempts_.load();
  }

private:
  int fd_ = -1;
  uint16_t port_ = 0;
  std::atomic<int> attempts_{0};
  std::thread worker_;
};

} // namespace

TEST("HttpPool stream_request retry_after_headers defaults to true and opts "
     "out cleanly") {
  // Default (`retry_after_headers = true`): a 0-byte body timeout after final
  // 200 headers retries and succeeds on attempt 2.
  {
    auto server = StreamRetryServer{2};
    auto retry_notifications = std::atomic<int>{0};
    auto config = HttpPoolConfig{
      .tls = {},
      .request_timeout = std::chrono::milliseconds{120},
      .connection_timeout = std::chrono::seconds{2},
      .max_retry_count = 2,
      .retry_delay = std::chrono::milliseconds{0},
      .max_concurrent_streams_per_connection = 1,
      .on_retry =
        [&](std::string_view) {
          retry_notifications.fetch_add(1);
        },
    };
    auto pool = HttpPool::make(folly::getGlobalIOExecutor(), server.url(),
                               std::move(config));
    REQUIRE(pool.is_ok());
    auto body = std::string{};
    auto callbacks = HttpStreamCallbacks{};
    callbacks.on_body = [&](std::string chunk) -> Task<bool> {
      body += chunk;
      co_return false;
    };
    auto client = std::move(pool).unwrap();
    auto result = folly::coro::blockingWait(client->stream_post(
      "/test", "", std::vector<http::Header>{}, std::move(callbacks)));
    REQUIRE(result.is_ok());
    CHECK_EQUAL(result.unwrap().status_code, uint16_t{200});
    CHECK_EQUAL(body, "ok");
    CHECK_EQUAL(server.attempts(), 2);
    CHECK_EQUAL(retry_notifications.load(), 1);
  }
  // Opt-out (`retry_after_headers = false`): a 0-byte body timeout after final
  // 200 headers must NOT retry or emit retry notifications.
  {
    auto server = StreamRetryServer{1};
    auto retry_notifications = std::atomic<int>{0};
    auto config = HttpPoolConfig{
      .tls = {},
      .request_timeout = std::chrono::milliseconds{120},
      .connection_timeout = std::chrono::seconds{2},
      .max_retry_count = 2,
      .retry_delay = std::chrono::seconds{30},
      .max_concurrent_streams_per_connection = 1,
      .on_retry =
        [&](std::string_view) {
          retry_notifications.fetch_add(1);
        },
    };
    auto pool = HttpPool::make(folly::getGlobalIOExecutor(), server.url(),
                               std::move(config));
    REQUIRE(pool.is_ok());
    auto received_status = Option<uint16_t>{None{}};
    auto callbacks = HttpStreamCallbacks{};
    callbacks.on_headers = [&](http::Response const& resp) {
      received_status = resp.status_code;
    };
    callbacks.retry_after_headers = false;
    auto client = std::move(pool).unwrap();
    auto result = folly::coro::blockingWait(client->stream_post(
      "/test", "", std::vector<http::Header>{}, std::move(callbacks)));
    REQUIRE(result.is_err());
    CHECK_EQUAL(received_status, Option<uint16_t>{200});
    CHECK_EQUAL(server.attempts(), 1);
    CHECK_EQUAL(retry_notifications.load(), 0);
  }
}
