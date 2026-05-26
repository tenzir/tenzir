//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/option.hpp"
#include "tenzir/proxy_settings.hpp"

#include <folly/coro/Task.h>
#include <proxygen/lib/http/coro/HTTPSourceHolder.h>
#include <proxygen/lib/http/coro/HTTPSourceReader.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>

#include <chrono>
#include <string>
#include <string_view>

namespace folly {
class EventBase;
} // namespace folly

namespace proxygen {
class HTTPHeaders;
class URL;
} // namespace proxygen

namespace tenzir {

enum class HttpRequestTargetForm {
  origin,
  absolute,
};

struct HttpProxyRequestContext {
  HttpRequestTargetForm target_form = HttpRequestTargetForm::origin;
  Option<proxy_url> proxy = None{};
};

class HttpProxyRequest {
public:
  HttpProxyRequest();
  ~HttpProxyRequest();
  HttpProxyRequest(HttpProxyRequest&&) noexcept;
  auto operator=(HttpProxyRequest&&) noexcept -> HttpProxyRequest&;
  HttpProxyRequest(HttpProxyRequest const&) = delete;
  auto operator=(HttpProxyRequest const&) -> HttpProxyRequest& = delete;
  auto target_form() const -> HttpRequestTargetForm;
  auto proxy() const -> Option<proxy_url>;
  auto send(proxygen::coro::HTTPSourceHolder source,
            proxygen::coro::HTTPSourceReader reader,
            std::chrono::milliseconds timeout) -> folly::coro::Task<void>;

private:
  struct State;
  explicit HttpProxyRequest(Box<State> state, HttpProxyRequestContext context);
  Box<State> state_;
  HttpProxyRequestContext context_;
  friend auto make_http_proxy_request(
    folly::EventBase& evb, std::string host, uint16_t port,
    proxygen::coro::HTTPCoroConnector::ConnectionParams direct_conn_params,
    proxygen::coro::HTTPCoroConnector::SessionParams session_params,
    std::chrono::milliseconds connect_timeout, bool target_is_secure)
    -> folly::coro::Task<HttpProxyRequest>;
};

/// Opens an HTTP request either directly or via the configured HTTP proxy.
///
/// When a proxy is configured for the target scheme, `host` is not on the
/// `tenzir.no-proxy` bypass list, and `target_is_secure` is true, this routes
/// through HTTP CONNECT:
///   1. Resolves DNS for the proxy host and opens a session to the
///      proxy. The session is TLS-wrapped when the proxy URL uses
///      `https://`, otherwise plain HTTP.
///   2. Issues `HTTPCoroConnector::proxyConnect` to perform the
///      CONNECT handshake to `(host, port)`. The TLS settings for
///      the *target* (i.e. when the target itself is HTTPS) come from
///      `direct_conn_params`.
///
/// When `target_is_secure` is false, this opens the session to the proxy and
/// returns `HttpRequestTargetForm::absolute`; callers must send an
/// absolute-form request target. Without a configured proxy, the behaviour is
/// identical to
/// `HTTPCoroConnector::connect` with a pre-resolved server address.
auto make_http_proxy_request(
  folly::EventBase& evb, std::string host, uint16_t port,
  proxygen::coro::HTTPCoroConnector::ConnectionParams direct_conn_params,
  proxygen::coro::HTTPCoroConnector::SessionParams session_params,
  std::chrono::milliseconds connect_timeout, bool target_is_secure)
  -> folly::coro::Task<HttpProxyRequest>;

/// Returns the request target to use for `target_form`.
///
/// Direct and CONNECT-tunnelled sessions use the usual origin-form target,
/// while cleartext forward-proxy sessions use absolute-form.
auto make_proxy_request_target(proxygen::URL const& url,
                               HttpRequestTargetForm target_form)
  -> std::string;

/// Returns the request target to use for a request path that may differ from
/// `url`.
auto make_proxy_request_target(proxygen::URL const& url,
                               std::string_view origin_form,
                               HttpRequestTargetForm target_form)
  -> std::string;

/// Adds proxy credentials to a cleartext forward-proxy request when configured.
///
/// CONNECT credentials are handled by `make_http_proxy_request`.
auto add_forward_proxy_authorization(proxygen::HTTPHeaders& headers,
                                     HttpRequestTargetForm target_form,
                                     Option<proxy_url> const& proxy) -> void;

} // namespace tenzir
