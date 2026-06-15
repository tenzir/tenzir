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
#include <tenzir/http.hpp>
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
#include <vector>

namespace folly {
class EventBase;
class IOExecutor;
class SSLContext;
} // namespace folly

namespace proxygen {
enum class HTTPMethod;
} // namespace proxygen

namespace tenzir {

using HttpHeaderFactory
  = std::function<Result<std::vector<http::Header>, std::string>()>;

/// Runtime settings for an `HttpPool`.
///
/// The pool owns the retry policy and TLS client context. Callers usually build
/// this through `http::make_http_pool_config` so URL normalization and TLS
/// diagnostics stay consistent across operators.
struct HttpPoolConfig {
  /// Whether the pool connects via HTTPS.
  bool tls = true;
  /// TLS client context used when `tls` is true.
  std::shared_ptr<folly::SSLContext> ssl_context;
  /// Time allowed for one complete HTTP request attempt.
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
  /// Time allowed for establishing a connection.
  std::chrono::milliseconds connection_timeout = std::chrono::seconds{5};
  /// Number of retries after the initial attempt.
  uint32_t max_retry_count = 0;
  /// Base delay between retries when the response does not override it.
  std::chrono::milliseconds retry_delay = std::chrono::seconds{1};
  /// Callback invoked before each retry sleep with a preformatted message.
  std::function<void(std::string_view message)> on_retry;
};

/// Callbacks for streaming an HTTP response body.
///
/// `stream_request` calls `on_headers` once after final response headers arrive
/// and before any body chunks are delivered. It then calls `on_body` for each
/// non-empty body chunk in wire order. Returning `true` from `on_body` stops
/// consuming the response early; returning `false` keeps reading until EOF or
/// error. Empty callbacks are allowed and simply skip the corresponding event.
struct HttpStreamCallbacks {
  /// Called once final response headers arrive, before the first body chunk.
  std::function<void(http::Response const& response)> on_headers;
  /// Called for every response body chunk.
  std::function<Task<bool>(std::string chunk)> on_body;
};

/// Registers well-known system CA bundle paths for Proxygen HTTPS clients.
///
/// Safe to call multiple times; initialization is performed once process-wide.

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

  /// Request through the session pool to a path.
  ///
  /// Path must start with `/` and may contain fragment and/or query.
  /// When None, it falls back to path of the pool URL.
  auto request(proxygen::HTTPMethod method, Option<std::string> path,
               std::string body, std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool to a path.
  auto request(proxygen::HTTPMethod method, Option<std::string> path,
               std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool to a path.
  auto request(proxygen::HTTPMethod method, Option<std::string> path,
               std::string body, HttpHeaderFactory make_headers)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool to a path.
  auto request(proxygen::HTTPMethod method, std::string body,
               std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool to a path.
  auto request(proxygen::HTTPMethod method, std::string body,
               std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool.
  auto post(std::string body, std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool.
  auto post(std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool to a path.
  auto
  post(std::string path, std::string body, std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool to a path.
  auto post(std::string path, std::string body,
            std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool to a path.
  auto post(std::string path, std::string body, HttpHeaderFactory make_headers)
    -> Task<Result<http::Response, std::string>>;

  /// GET through the session pool to a path.
  auto get(std::string path, std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// GET through the session pool to a path.
  auto get(std::string path, std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool while streaming response body chunks.
  auto stream_request(proxygen::HTTPMethod method, std::string path,
                      std::string body, std::vector<http::Header> headers,
                      HttpStreamCallbacks callbacks)
    -> Task<Result<http::Response, std::string>>;

  /// Request through the session pool while streaming response body chunks.
  auto stream_request(proxygen::HTTPMethod method, std::string path,
                      std::string body, HttpHeaderFactory make_headers,
                      HttpStreamCallbacks callbacks)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool while streaming response body chunks.
  auto
  stream_post(std::string path, std::string body,
              std::vector<http::Header> headers, HttpStreamCallbacks callbacks)
    -> Task<Result<http::Response, std::string>>;

  /// POST through the session pool while streaming response body chunks.
  auto
  stream_post(std::string path, std::string body,
              HttpHeaderFactory make_headers, HttpStreamCallbacks callbacks)
    -> Task<Result<http::Response, std::string>>;

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
               std::vector<http::Header> headers,
               std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

/// One-shot HTTP POST without a connection pool.
auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

/// One-shot HTTP GET without a connection pool.
auto http_get(folly::EventBase* evb, std::string url,
              std::vector<http::Header> headers,
              std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

/// One-shot HTTP GET without a connection pool.
auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

} // namespace tenzir
