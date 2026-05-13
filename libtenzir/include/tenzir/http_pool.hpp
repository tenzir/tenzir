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
#include <tenzir/option.hpp>
#include <tenzir/result.hpp>

#include <folly/Executor.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace folly {
class EventBase;
class IOExecutor;
class SSLContext;
} // namespace folly

// namespace proxygen::coro
namespace proxygen {
enum class HTTPMethod;
} // namespace proxygen

namespace tenzir {

struct HttpResponse {
  uint16_t status_code = 0;
  std::map<std::string, std::string> headers;
  std::string body;

  auto is_status_success() const -> bool {
    return status_code >= 200 and status_code < 300;
  }
};

struct HttpPoolConfig {
  bool tls = true;
  std::shared_ptr<folly::SSLContext> ssl_context;
  std::optional<std::string> ca_info;
  bool skip_peer_verification = false;
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
  std::chrono::milliseconds connection_timeout = std::chrono::seconds{5};
  uint32_t max_retry_count = 0;
  std::chrono::milliseconds retry_delay = std::chrono::seconds{1};
  /// callback invoked before each retry sleep with a preformatted message
  std::function<void(std::string_view message)> on_retry;
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
  auto request(proxygen::HTTPMethod method, std::string body,
               std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// Request through the session pool to a path.
  ///
  /// Path must start with `/` and may contain fragment and/or query.
  /// When None, it falls back to path of the pool URL.
  auto request(proxygen::HTTPMethod method, Option<std::string> path,
               std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// POST through the session pool.
  auto post(std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

  /// POST through the session pool to a path.
  auto post(std::string path, std::string body,
            std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

private:
  explicit HttpPool(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                    std::string url, HttpPoolConfig config);

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

/// One-shot HTTP POST without a connection pool.
///
/// Automatically dispatches the request on the given event base thread.
auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               HttpPoolConfig config)
  -> Task<Result<HttpResponse, std::string>>;

/// One-shot HTTP GET without a connection pool.
auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<HttpResponse, std::string>>;

/// One-shot HTTP GET without a connection pool.
auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers, HttpPoolConfig config)
  -> Task<Result<HttpResponse, std::string>>;

} // namespace tenzir
