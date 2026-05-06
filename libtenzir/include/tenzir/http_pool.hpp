//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/result.hpp>

#include <folly/Executor.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>

namespace folly {
class EventBase;
class IOExecutor;
class SSLContext;
} // namespace folly

namespace proxygen::coro {
enum class HTTPErrorCode : uint16_t;
} // namespace proxygen::coro

namespace tenzir {

namespace http {

auto is_retryable_http_error(proxygen::coro::HTTPErrorCode code) -> bool;

auto is_retryable_http_status(uint16_t status_code) -> bool;

struct retryable_http_response : std::runtime_error {
  explicit retryable_http_response(
    uint16_t status_code = 0,
    std::vector<std::pair<std::string, std::string>> headers = {},
    std::string body = {});

  uint16_t status_code = 0;
  std::vector<std::pair<std::string, std::string>> headers;
  std::string body;
};

} // namespace http

struct HttpResponse {
  uint16_t status_code = 0;
  std::map<std::string, std::string> headers;
  std::string body;
};

struct HttpPoolConfig {
  bool tls = true;
  std::shared_ptr<folly::SSLContext> ssl_context;
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
  std::chrono::milliseconds connection_timeout = std::chrono::seconds{5};
  uint32_t max_retry_count = 0;
  std::chrono::milliseconds retry_delay = std::chrono::seconds{1};
};

/// Registers well-known system CA bundle paths for Proxygen HTTPS clients.
///
/// Safe to call multiple times; initialization is performed once process-wide.
auto ensure_http_default_ca_paths() -> void;

/// Coroutine-friendly HTTP connection pool backed by proxygen.
///
/// Requests are automatically dispatched on the pool's event base thread.
/// Use `HttpPool::make()` to construct; this ensures proper destruction on
/// the event base thread.
class HttpPool {
public:
  /// Creates a pool bound to the given IO executor and its current event base.
  static auto make(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                   std::string url, HttpPoolConfig config = {})
    -> Box<HttpPool>;

  ~HttpPool();
  HttpPool(HttpPool const&) = delete;
  auto operator=(HttpPool const&) -> HttpPool& = delete;
  HttpPool(HttpPool&&) noexcept;
  auto operator=(HttpPool&&) noexcept -> HttpPool&;

  /// Request through the session pool.
  auto request(std::string method, std::string body,
               std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// Request through the session pool with an origin-form target.
  ///
  /// The target must start with `/` and may contain a query string. The request
  /// still uses the scheme, host, and port from the pool URL.
  auto request(std::string method, std::string target, std::string body,
               std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// POST through the session pool.
  auto post(std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// POST through the session pool with an origin-form target.
  auto post(std::string target, std::string body,
            std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

private:
  explicit HttpPool(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                    std::string url, HttpPoolConfig config);

  auto request(std::string method, std::optional<std::string> target,
               std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  struct Impl;
  std::shared_ptr<Impl> impl_;
};

/// One-shot HTTP POST without a connection pool.
///
/// Automatically dispatches the request on the given event base thread.
auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<HttpResponse, std::string>>;

/// One-shot HTTP GET without a connection pool.
auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<HttpResponse, std::string>>;

} // namespace tenzir
