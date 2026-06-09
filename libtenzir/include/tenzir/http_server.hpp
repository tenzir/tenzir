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

#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tls_options.hpp"

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
                    const caf::actor_system_config& cfg) -> bool;

auto make_response(uint16_t status, const std::string& content_type,
                   std::string body) -> proxygen::coro::HTTPSourceHolder;

/// Drop-in replacement for `proxygen::coro::ScopedHTTPServer` whose destructor
/// is safe when `start()` fails. The upstream version joins its IO thread both
/// in `start()` (after a bind failure) and again in the destructor, which
/// throws `std::system_error` during stack unwinding and terminates the
/// process.
class scoped_server {
public:
  static auto start(proxygen::coro::HTTPServer::Config config,
                    std::shared_ptr<proxygen::coro::HTTPHandler> handler)
    -> std::unique_ptr<scoped_server>;

  ~scoped_server();

  scoped_server(scoped_server const&) = delete;
  scoped_server(scoped_server&&) = delete;
  auto operator=(scoped_server const&) -> scoped_server& = delete;
  auto operator=(scoped_server&&) -> scoped_server& = delete;

  auto getServer() -> proxygen::coro::HTTPServer& {
    return server_;
  }

private:
  scoped_server(proxygen::coro::HTTPServer::Config config,
                std::shared_ptr<proxygen::coro::HTTPHandler> handler);

  void start_impl();

  proxygen::coro::HTTPServer server_;
  std::thread thread_;
};

} // namespace tenzir::http_server
