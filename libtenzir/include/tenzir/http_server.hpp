//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This header exposes the proxygen-dependent HTTP server utilities.
// It is intentionally NOT included by http.hpp to avoid pulling in proxygen
// headers everywhere. Only include this from operators that actually run an
// HTTP server (accept_http, accept_opensearch) and from http_server.cpp.

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/result.hpp"
#include "tenzir/tls_options.hpp"

#include <folly/Function.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/server/HTTPServer.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <charconv>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <thread>

namespace tenzir::http_server {

/// Builds a wangle SSL context config from a resolved TLS configuration.
auto make_ssl_context_config(TlsConfig const& tls, location primary,
                             diagnostic_handler& dh)
  -> failure_or<wangle::SSLContextConfig>;

template <class T>
auto parse_number(std::string_view text) -> Option<T> {
  if (text.empty()) {
    return None{};
  }
  auto value = T{};
  auto const* begin = text.data();
  auto const* end = begin + text.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} or ptr != end) {
    return None{};
  }
  return value;
}

struct server_endpoint {
  std::string host;
  uint16_t port;
  Option<bool> scheme_tls;
};

auto parse_endpoint(std::string_view endpoint, location loc,
                    diagnostic_handler& dh,
                    std::string_view argument_name = "endpoint")
  -> Option<server_endpoint>;

auto is_tls_enabled(Option<located<data>> const& tls,
                    caf::actor_system_config const& cfg) -> bool;

/// Builds the common Proxygen server configuration, including endpoint and
/// TLS validation.
auto make_config(std::string_view endpoint, location endpoint_location,
                 Option<located<data>> const& tls,
                 caf::actor_system_config const& cfg, diagnostic_handler& dh,
                 std::string_view argument_name = "endpoint")
  -> failure_or<proxygen::coro::HTTPServer::Config>;

auto make_response(uint16_t status, const std::string& content_type,
                   std::string body) -> proxygen::coro::HTTPSourceHolder;

/// Invokes a callback once an HTTP response is delivered or canceled.
auto track_response_delivery(proxygen::coro::HTTPSourceHolder response,
                             folly::Function<void()> callback)
  -> proxygen::coro::HTTPSourceHolder;

/// Invokes one callback when the response reaches the kernel write and another
/// once delivery completes or is canceled.
auto track_response_delivery(proxygen::coro::HTTPSourceHolder response,
                             folly::Function<void()> kernel_write_callback,
                             folly::Function<void()> delivery_callback)
  -> proxygen::coro::HTTPSourceHolder;

/// RAII wrapper around `proxygen::coro::HTTPServer` that runs the server on a
/// dedicated IO thread and tears it down on destruction. Unlike upstream's
/// `proxygen::coro::ScopedHTTPServer`, `start()` catches bind failures and
/// returns an error instead of throwing across the API, and the destructor is
/// safe even when the server never reached the running state.
class ScopedServer {
public:
  static auto start(proxygen::coro::HTTPServer::Config config,
                    std::shared_ptr<proxygen::coro::HTTPHandler> handler)
    -> Result<std::unique_ptr<ScopedServer>, std::string>;

  ~ScopedServer();

  ScopedServer(ScopedServer const&) = delete;
  ScopedServer(ScopedServer&&) = delete;
  auto operator=(ScopedServer const&) -> ScopedServer& = delete;
  auto operator=(ScopedServer&&) -> ScopedServer& = delete;

  auto server() -> proxygen::coro::HTTPServer& {
    return server_;
  }

private:
  ScopedServer(proxygen::coro::HTTPServer::Config config,
               std::shared_ptr<proxygen::coro::HTTPHandler> handler);

  void start_impl();

  proxygen::coro::HTTPServer server_;
  std::thread thread_;
};

/// Non-blocking owner for a running HTTP server.
///
/// Destruction force-stops the server and moves its joining destructor to a
/// detached thread.
class Server {
public:
  static auto start(proxygen::coro::HTTPServer::Config config,
                    std::shared_ptr<proxygen::coro::HTTPHandler> handler)
    -> Task<Result<Box<Server>, std::string>>;

  explicit Server(std::unique_ptr<ScopedServer> server) noexcept;
  ~Server();

  Server(Server const&) = delete;
  Server(Server&&) = delete;
  auto operator=(Server const&) -> Server& = delete;
  auto operator=(Server&&) -> Server& = delete;

  auto drain() -> void;
  auto finish() -> void;
  auto force_stop() -> void;

private:
  Option<Box<ScopedServer>> server_;
};

} // namespace tenzir::http_server
