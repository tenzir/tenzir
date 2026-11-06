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

#include <folly/ExceptionWrapper.h>
#include <folly/Executor.h>

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <stdexcept>
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

/// TLS settings for the servers that HTTP clients connect to.
///
/// The settings do not depend on a particular server. Every `HttpPool` derives
/// a TLS context that verifies the certificate of its own host, so one
/// `HttpPoolConfig` can serve pools for different hosts.
class HttpClientTls {
public:
  /// Uses the system trust store and default verification.
  HttpClientTls() = default;

  /// Uses resolved TLS settings.
  explicit HttpClientTls(TlsConfig config);

  /// Uses resolved TLS settings and prepares a context for `host`.
  ///
  /// Preparing the context reports configuration errors, such as an unreadable
  /// `cacert`, with their source locations. Pools for `host` reuse it.
  static auto make(TlsConfig config, std::string_view host,
                   diagnostic_handler& dh) -> failure_or<HttpClientTls>;

  /// Returns these settings without the client certificate and key.
  ///
  /// Use this for servers that should not receive the client identity, such
  /// as an OAuth token endpoint next to the configured API server.
  auto without_client_identity() const -> HttpClientTls;

  /// Returns a TLS context that verifies the certificate of `host`, or
  /// `nullptr` for the system defaults.
  auto context_for(std::string_view host) const
    -> Result<std::shared_ptr<folly::SSLContext>, std::string>;

private:
  std::shared_ptr<TlsConfig const> config_;
  std::string prepared_host_;
  std::shared_ptr<folly::SSLContext> prepared_;
};

/// A server certificate failed verification. The message names the server and
/// the reason, like the result of `describe_tls_verification_error`.
class TlsVerificationError : public std::runtime_error {
public:
  using std::runtime_error::runtime_error;
};

/// Describes why a server certificate failed verification, if `error` is a
/// handshake that failed this way, possibly wrapped in a failed connection
/// attempt of a session pool.
///
/// The failure must come from a connection whose context has
/// `record_peer_verification`. `host` names the destination and `proxy_host`
/// the HTTPS proxy, if any. A `TlsVerificationError` describes itself.
auto describe_tls_verification_error(folly::exception_wrapper const& error,
                                     std::string_view host,
                                     Option<std::string_view> proxy_host
                                     = None{}) -> Option<std::string>;

/// Regenerates headers before each buffered or streaming request attempt.
/// Invoked serially on the pool's IO executor, including after retry sleeps.
/// Producers that need no asynchronous work can simply co_return their result.
using HttpHeaderFactory
  = std::function<Task<Result<std::vector<http::Header>, std::string>>()>;

/// Runtime settings for an `HttpPool`.
///
/// The pool owns the retry policy and TLS client context. Callers usually build
/// this through `http::make_http_pool_config` so URL normalization and TLS
/// diagnostics stay consistent across operators. The URL scheme of a pool
/// decides whether it uses TLS.
struct HttpPoolConfig {
  /// TLS settings for pools with an `https` URL.
  HttpClientTls tls;
  /// Time allowed for one complete HTTP request attempt.
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
  /// Time allowed for establishing a connection.
  std::chrono::milliseconds connection_timeout = std::chrono::seconds{5};
  /// Number of retries after the initial attempt.
  uint32_t max_retry_count = 0;
  /// Base delay between retries when the response does not override it.
  std::chrono::milliseconds retry_delay = std::chrono::seconds{1};
  /// Maximum concurrent requests per connection, or 0 for the protocol
  /// default. Set to 1 to disable HTTP/1.1 pipelining so that concurrent
  /// requests fan out over multiple connections instead of serializing
  /// head-of-line on a single one.
  uint32_t max_concurrent_streams_per_connection = 0;
  /// Callback invoked before each retry sleep with a preformatted message.
  std::function<void(std::string_view message)> on_retry;
};

/// Callbacks for streaming an HTTP response body.
///
/// `stream_request` calls `on_headers` after final response headers arrive
/// and before any body chunks are delivered. It then calls `on_body` for each
/// non-empty body chunk in wire order. Returning `true` from `on_body` stops
/// consuming the response early; returning `false` keeps reading until EOF or
/// error. Empty callbacks are allowed and simply skip the corresponding event.
struct HttpStreamCallbacks {
  /// Called after final response headers arrive in each attempt, before its body.
  std::function<void(http::Response const& response)> on_headers;
  /// Called for every response body chunk.
  std::function<Task<bool>(std::string chunk)> on_body;
  /// Roll back response side effects before retrying a failed attempt.
  /// Providing this callback permits retries after partial response bodies were
  /// delivered.
  std::function<void()> on_retry;
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
  ///
  /// Fails for URLs without a host, when the TLS setup for the server or its
  /// HTTPS proxy fails, such as for a missing CA bundle, and when the host of
  /// the server or proxy does not resolve. The error never contains the URL,
  /// which may hold credentials.
  [[nodiscard]] static auto
  make(folly::Executor::KeepAlive<folly::IOExecutor> executor, std::string url,
       HttpPoolConfig config = {}) -> Result<Box<HttpPool>, std::string>;

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

  /// Request with regenerated headers on every attempt.
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

  /// POST with regenerated headers on every attempt.
  auto post(std::string path, std::string body, HttpHeaderFactory make_headers)
    -> Task<Result<http::Response, std::string>>;

  /// GET through the session pool to a path.
  auto get(std::string path, std::vector<http::Header> headers)
    -> Task<Result<http::Response, std::string>>;

  /// GET through the session pool to a path.
  auto get(std::string path, std::map<std::string, std::string> headers)
    -> Task<Result<http::Response, std::string>>;

  /// GET through the session pool while streaming response body chunks.
  auto stream_get(std::string path, std::vector<http::Header> headers,
                  HttpStreamCallbacks callbacks)
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
  struct Impl;

  explicit HttpPool(std::shared_ptr<Impl> impl);

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

/// One-shot HTTP DELETE without a connection pool.
auto http_delete(folly::EventBase* evb, std::string url,
                 std::vector<http::Header> headers,
                 std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

/// One-shot HTTP DELETE without a connection pool.
auto http_delete(folly::EventBase* evb, std::string url,
                 std::map<std::string, std::string> headers,
                 std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<http::Response, std::string>>;

} // namespace tenzir
