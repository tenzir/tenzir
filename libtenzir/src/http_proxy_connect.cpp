//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_proxy_connect.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/http.hpp"
#include "tenzir/proxy_settings.hpp"

#include <fmt/format.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/lib/http/coro/client/CoroDNSResolver.h>

namespace tenzir {

namespace {

auto make_connect_headers()
  -> proxygen::coro::HTTPCoroConnector::ConnectHeaderMap {
  auto const& ps = get_proxy_settings();
  auto headers = proxygen::coro::HTTPCoroConnector::ConnectHeaderMap{};
  if (not ps.proxy_username) {
    return headers;
  }
  auto credentials
    = fmt::format("{}:{}", *ps.proxy_username,
                  ps.proxy_password ? std::string_view{*ps.proxy_password}
                                    : std::string_view{});
  headers.emplace("Proxy-Authorization",
                  fmt::format("Basic {}", detail::base64::encode(credentials)));
  return headers;
}

} // namespace

auto connect_session_via_proxy_if_configured(
  folly::EventBase* evb, std::string host, uint16_t port,
  proxygen::coro::HTTPCoroConnector::ConnectionParams direct_conn_params,
  proxygen::coro::HTTPCoroConnector::SessionParams session_params,
  std::chrono::milliseconds connect_timeout)
  -> folly::coro::Task<proxygen::coro::HTTPCoroSession*> {
  auto const& ps = get_proxy_settings();
  auto proxy_url = proxy_for_host(host);
  if (not proxy_url) {
    // Direct connect path — identical to what the operator does today.
    auto addresses = co_await proxygen::coro::CoroDNSResolver::resolveHost(
      evb, host, connect_timeout);
    auto server_addr = std::move(addresses.primary);
    server_addr.setPort(port);
    co_return co_await proxygen::coro::HTTPCoroConnector::connect(
      evb, std::move(server_addr), connect_timeout,
      std::move(direct_conn_params), std::move(session_params));
  }
  TENZIR_ASSERT(ps.proxy_host and ps.proxy_port and ps.proxy_scheme);
  // Step 1: resolve and connect to the proxy itself. When the proxy
  // URL scheme is `https`, the connection to the proxy is TLS-wrapped
  // before any CONNECT request is sent. The CONNECT tunnel then
  // double-wraps with the target's own TLS if `direct_conn_params`
  // requested it.
  auto proxy_secure = *ps.proxy_scheme == "https"
                        ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                        : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
  if (proxy_secure == proxygen::coro::HTTPClient::SecureTransportImpl::TLS) {
    http::ensure_default_ca_paths();
  }
  auto proxy_addresses = co_await proxygen::coro::CoroDNSResolver::resolveHost(
    evb, *ps.proxy_host, connect_timeout);
  auto proxy_addr = std::move(proxy_addresses.primary);
  proxy_addr.setPort(*ps.proxy_port);
  auto proxy_conn_params
    = proxygen::coro::HTTPClient::getConnParams(proxy_secure, *ps.proxy_host);
  auto* proxy_session = co_await proxygen::coro::HTTPCoroConnector::connect(
    evb, std::move(proxy_addr), connect_timeout, std::move(proxy_conn_params),
    proxygen::coro::HTTPCoroConnector::defaultSessionParams());
  // Step 2: CONNECT to the target through the proxy session. The resulting
  // tunnelled session owns its own transport, so the proxy session is consumed
  // (connectUnique = true).
  auto reservation = proxy_session->reserveRequest();
  if (reservation.hasException()) {
    co_yield folly::coro::co_error(std::move(reservation.exception()));
  }
  auto authority = fmt::format("{}:{}", host, port);
  // `direct_conn_params` carries the TLS settings for the *target*;
  // pass them as-is so the CONNECT tunnel TLS-wraps the inner stream
  // when the target itself is HTTPS.
  co_return co_await proxygen::coro::HTTPCoroConnector::proxyConnect(
    proxy_session, std::move(*reservation), std::move(authority),
    /*connectUnique=*/true, connect_timeout, std::move(direct_conn_params),
    std::move(session_params), make_connect_headers());
}

} // namespace tenzir
