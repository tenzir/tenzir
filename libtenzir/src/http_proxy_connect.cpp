//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/http_proxy_connect.hpp"

#include "tenzir/concept/parseable/tenzir/ip.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/http.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/proxy_settings.hpp"
#include "tenzir/ref.hpp"

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/SocketAddress.h>
#include <folly/io/async/EventBase.h>
#include <proxygen/lib/http/HTTPHeaders.h>
#include <proxygen/lib/http/coro/client/CoroDNSResolver.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/utils/URL.h>

namespace tenzir {

namespace {

auto make_proxy_authorization_value(proxy_url const& proxy)
  -> Option<std::string> {
  if (not proxy.username) {
    return {};
  }
  auto credentials = fmt::format(
    "{}:{}", *proxy.username,
    proxy.password ? std::string_view{*proxy.password} : std::string_view{});
  return fmt::format("Basic {}", detail::base64::encode(credentials));
}

auto make_connect_headers(proxy_url const& proxy)
  -> proxygen::coro::HTTPCoroConnector::ConnectHeaderMap {
  auto headers = proxygen::coro::HTTPCoroConnector::ConnectHeaderMap{};
  if (auto authorization = make_proxy_authorization_value(proxy)) {
    headers.emplace("Proxy-Authorization", *authorization);
  }
  return headers;
}

auto format_authority(std::string_view host, uint16_t port) -> std::string {
  auto unbracketed_host = host;
  if (unbracketed_host.size() >= 2 and unbracketed_host.front() == '['
      and unbracketed_host.back() == ']') {
    unbracketed_host.remove_prefix(1);
    unbracketed_host.remove_suffix(1);
  }
  if (auto address = to<ip>(std::string{unbracketed_host});
      address and address->is_v6()) {
    return fmt::format("[{}]:{}", unbracketed_host, port);
  }
  return fmt::format("{}:{}", unbracketed_host, port);
}

struct HttpProxySession {
  Option<Ref<proxygen::coro::HTTPCoroSession>> session = None{};
  HttpProxyRequestContext context;
};

auto open_http_proxy_session(
  folly::EventBase& evb, std::string host, uint16_t port,
  proxygen::coro::HTTPCoroConnector::ConnectionParams direct_conn_params,
  proxygen::coro::HTTPCoroConnector::SessionParams session_params,
  std::chrono::milliseconds connect_timeout, bool target_is_secure)
  -> folly::coro::Task<HttpProxySession> {
  auto proxy = proxy_for_target(target_is_secure ? "https" : "http", host);
  if (not proxy) {
    // Direct path: resolve the target host and connect to it.
    auto addresses = co_await proxygen::coro::CoroDNSResolver::resolveHost(
      &evb, host, connect_timeout);
    auto server_addr = std::move(addresses.primary);
    server_addr.setPort(port);
    auto& session = *co_await proxygen::coro::HTTPCoroConnector::connect(
      &evb, std::move(server_addr), connect_timeout,
      std::move(direct_conn_params), std::move(session_params));
    co_return HttpProxySession{
      .session = session,
      .context = {
        .target_form = HttpRequestTargetForm::origin,
        .proxy = None{},
      },
    };
  }
  // Step 1: resolve and connect to the proxy itself. When the proxy
  // URL scheme is `https`, the connection to the proxy is TLS-wrapped
  // before any CONNECT request is sent. The CONNECT tunnel then
  // double-wraps with the target's own TLS if `direct_conn_params`
  // requested it.
  auto proxy_secure = proxy->scheme == "https"
                        ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                        : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
  if (proxy_secure == proxygen::coro::HTTPClient::SecureTransportImpl::TLS) {
    http::ensure_default_ca_paths();
  }
  auto proxy_addresses = co_await proxygen::coro::CoroDNSResolver::resolveHost(
    &evb, proxy->host, connect_timeout);
  auto proxy_addr = std::move(proxy_addresses.primary);
  proxy_addr.setPort(proxy->port);
  auto proxy_conn_params
    = proxygen::coro::HTTPClient::getConnParams(proxy_secure, proxy->host);
  auto& proxy_session = *co_await proxygen::coro::HTTPCoroConnector::connect(
    &evb, std::move(proxy_addr), connect_timeout, std::move(proxy_conn_params),
    proxygen::coro::HTTPCoroConnector::defaultSessionParams());
  if (not target_is_secure) {
    co_return HttpProxySession{
      .session = proxy_session,
      .context = {
        .target_form = HttpRequestTargetForm::absolute,
        .proxy = proxy,
      },
    };
  }
  // Step 2: CONNECT to the target through the proxy session. The resulting
  // tunnelled session owns its own transport, so the proxy session is consumed
  // (connectUnique = true).
  auto reservation = proxy_session.reserveRequest();
  if (reservation.hasException()) {
    co_yield folly::coro::co_error(std::move(reservation.exception()));
  }
  auto authority = format_authority(host, port);
  // `direct_conn_params` carries the TLS settings for the *target*;
  // pass them as-is so the CONNECT tunnel TLS-wraps the inner stream
  // when the target itself is HTTPS.
  auto& session = *co_await proxygen::coro::HTTPCoroConnector::proxyConnect(
    &proxy_session, std::move(*reservation), std::move(authority),
    /*connectUnique=*/true, connect_timeout, std::move(direct_conn_params),
    std::move(session_params), make_connect_headers(*proxy));
  co_return HttpProxySession{
    .session = session,
    .context = {
      .target_form = HttpRequestTargetForm::origin,
      .proxy = None{},
    },
  };
}

} // namespace

struct HttpProxyRequest::State {
  State(proxygen::coro::HTTPCoroSession& session,
        proxygen::coro::HTTPCoroSession::RequestReservation reservation,
        proxygen::coro::HTTPSessionContextPtr holder)
    : session{session},
      reservation{std::move(reservation)},
      holder{std::move(holder)} {
  }
  ~State() {
    if (holder) {
      holder->initiateDrain();
    }
  }
  Ref<proxygen::coro::HTTPCoroSession> session;
  proxygen::coro::HTTPCoroSession::RequestReservation reservation;
  proxygen::coro::HTTPSessionContextPtr holder;
};

HttpProxyRequest::HttpProxyRequest() = default;
HttpProxyRequest::~HttpProxyRequest() = default;
HttpProxyRequest::HttpProxyRequest(HttpProxyRequest&&) noexcept = default;
auto HttpProxyRequest::operator=(HttpProxyRequest&&) noexcept
  -> HttpProxyRequest& = default;

HttpProxyRequest::HttpProxyRequest(Box<State> state,
                                   HttpProxyRequestContext context)
  : state_{std::move(state)}, context_{context} {
}

auto HttpProxyRequest::target_form() const -> HttpRequestTargetForm {
  return context_.target_form;
}

auto HttpProxyRequest::proxy() const -> Option<proxy_url> {
  return context_.proxy;
}

auto HttpProxyRequest::send(proxygen::coro::HTTPSourceHolder source,
                            proxygen::coro::HTTPSourceReader reader,
                            std::chrono::milliseconds timeout)
  -> folly::coro::Task<void> {
  co_await proxygen::coro::HTTPClient::request(&state_->session.get(),
                                               std::move(state_->reservation),
                                               std::move(source),
                                               std::move(reader), timeout);
}

auto make_http_proxy_request(
  folly::EventBase& evb, std::string host, uint16_t port,
  proxygen::coro::HTTPCoroConnector::ConnectionParams direct_conn_params,
  proxygen::coro::HTTPCoroConnector::SessionParams session_params,
  std::chrono::milliseconds connect_timeout, bool target_is_secure)
  -> folly::coro::Task<HttpProxyRequest> {
  auto connection = co_await open_http_proxy_session(
    evb, std::move(host), port, std::move(direct_conn_params),
    std::move(session_params), connect_timeout, target_is_secure);
  TENZIR_ASSERT(connection.session);
  auto& session = connection.session->get();
  auto holder = session.acquireKeepAlive();
  auto moved_to_request = false;
  SCOPE_EXIT {
    if (not moved_to_request) {
      if (holder) {
        holder->initiateDrain();
      }
    }
  };
  auto reservation = session.reserveRequest();
  if (reservation.hasException()) {
    co_yield folly::coro::co_error(std::move(reservation.exception()));
  }
  auto state = Box<HttpProxyRequest::State>{
    std::in_place,
    session,
    std::move(*reservation),
    std::move(holder),
  };
  moved_to_request = true;
  co_return HttpProxyRequest{std::move(state), connection.context};
}

auto make_proxy_request_target(proxygen::URL const& url,
                               HttpRequestTargetForm target_form)
  -> std::string {
  if (target_form == HttpRequestTargetForm::absolute) {
    return url.getUrl();
  }
  return url.makeRelativeURL();
}

auto make_proxy_request_target(proxygen::URL const& url,
                               std::string_view origin_form,
                               HttpRequestTargetForm target_form)
  -> std::string {
  if (target_form != HttpRequestTargetForm::absolute) {
    return std::string{origin_form};
  }
  if (origin_form.empty()) {
    origin_form = "/";
  }
  if (origin_form.front() != '/') {
    return fmt::format("{}://{}/{}", url.getScheme(),
                       format_authority(url.getHost(), url.getPort()),
                       origin_form);
  }
  return fmt::format("{}://{}{}", url.getScheme(),
                     format_authority(url.getHost(), url.getPort()),
                     origin_form);
}

auto add_forward_proxy_authorization(proxygen::HTTPHeaders& headers,
                                     HttpRequestTargetForm target_form,
                                     Option<proxy_url> const& proxy) -> void {
  if (target_form != HttpRequestTargetForm::absolute or not proxy) {
    return;
  }
  if (auto authorization = make_proxy_authorization_value(*proxy)) {
    headers.set("Proxy-Authorization", *authorization);
  }
}

} // namespace tenzir
