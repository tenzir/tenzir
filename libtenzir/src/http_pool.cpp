//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/result.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/http_proxy_connect.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/proxy_settings.hpp>
#include <tenzir/ref.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/try.hpp>

#include <fmt/format.h>
#include <folly/coro/Coroutine.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/async/SSLContext.h>
#include <folly/io/async/ssl/SSLErrors.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <stdexcept>
#include <system_error>

namespace tenzir::http {
auto to_http_response(proxygen::coro::HTTPClient::Response& resp) -> Response;
} // namespace tenzir::http

namespace tenzir {

namespace {

auto to_header_vector(std::map<std::string, std::string> headers)
  -> std::vector<http::Header> {
  auto result = std::vector<http::Header>{};
  result.reserve(headers.size());
  for (auto& [name, value] : headers) {
    result.push_back({std::move(name), std::move(value)});
  }
  return result;
}

/// Builds a proxygen request source from a URL, headers, and optional body.
///
/// This is our own version of proxygen's internal `makeHTTPRequestSource`,
/// which has a bug: it passes `strPtr->data()`, `strPtr->size()`, and
/// `strPtr.release()` as arguments to the same function call. Because C++
/// does not specify argument evaluation order, the compiler may evaluate
/// `release()` first, making `data()`/`size()` dereference a null unique_ptr.
auto make_request_source(proxygen::URL const& host_url, std::string path,
                         proxygen::HTTPMethod method,
                         std::span<http::Header const> headers,
                         Option<std::string> body,
                         HttpRequestTargetForm target_form,
                         Option<proxy_url> const& proxy)
  -> proxygen::coro::HTTPSource* {
  auto body_buf = std::unique_ptr<folly::IOBuf>{};
  if (body and not body->empty()) {
    body_buf = folly::IOBuf::fromString(std::move(*body));
  }
  path = make_proxy_request_target(host_url, path, target_form);
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedRequest(
    std::move(path), method, std::move(body_buf));
  for (auto const& header : headers) {
    source->msg_->getHeaders().add(header.name, header.value);
  }
  add_forward_proxy_authorization(source->msg_->getHeaders(), target_form,
                                  proxy);
  if (not source->msg_->getHeaders().exists(proxygen::HTTP_HEADER_HOST)) {
    source->msg_->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                                   host_url.getHostAndPortOmitDefault());
  }
  source->msg_->setWantsKeepalive(true);
  source->msg_->setSecure(host_url.isSecure());
  return source;
}

/// Retries `f` per `config`. `describe_tls_error` names a certificate that
/// failed verification for a failed attempt, which then is not retried.
template <class F, class D>
auto retry_request(HttpPoolConfig const& config, D const& describe_tls_error,
                   F&& f) -> Task<Result<http::Response, std::string>> {
  auto attempt = uint32_t{0};
  while (true) {
    auto retry_after = Option<std::chrono::seconds>{};
    auto retry_reason = std::string{};
    auto attempt_res
      = co_await async_try([&]() -> Task<proxygen::coro::HTTPClient::Response> {
          co_return co_await f();
        }());
    if (attempt_res.is_ok()) {
      // got response
      proxygen::coro::HTTPClient::Response resp
        = std::move(attempt_res).unwrap();
      if (not resp.headers) {
        // Proxygen can complete a request without a final response when the
        // stream is torn down mid-flight, e.g. by cancellation during
        // shutdown. Propagate cancellation; otherwise treat the missing
        // response like a connection error.
        co_await folly::coro::co_safe_point;
        if (attempt >= config.max_retry_count) {
          co_return Err{std::string{"request completed without a response"}};
        }
        retry_reason = "connection error";
      } else {
        auto const status = resp.headers->getStatusCode();
        if (attempt >= config.max_retry_count
            or not http::is_retryable_http_status(status)) {
          // not retryable
          co_return http::to_http_response(resp);
        }
        // retryable HTTP status
        retry_reason = fmt::format("HTTP error {}", status);
        retry_after = http::parse_retry_after(
          resp.headers->getHeaders().getSingleOrEmpty("Retry-After"));
      }
    } else {
      // transport error
      auto attempt_err = attempt_res.unwrap_err();
      // Retrying cannot fix a certificate that fails verification.
      if (auto error = describe_tls_error(attempt_err)) {
        co_return Err{std::move(*error)};
      }
      auto is_retryable = false;
      attempt_err.with_exception([&](folly::AsyncSocketException const&) {
        is_retryable = true;
      });
      attempt_err.with_exception([&](proxygen::coro::HTTPError const& err) {
        is_retryable = http::is_retryable_http_error(err.code);
      });
      attempt_err.with_exception(
        [&](proxygen::coro::HTTPCoroSessionPool::Exception const& err) {
          using Type = proxygen::coro::HTTPCoroSessionPool::Exception::Type;
          is_retryable = err.type == Type::Timeout;
        });
      if (not is_retryable or attempt >= config.max_retry_count) {
        // will not retry, return error
        co_return Err{std::move(attempt_err).what().toStdString()};
      }
      retry_reason = "connection error";
    }
    // will retry, compute delay and notify caller
    auto delay
      = http::retry_delay_for_attempt(config.retry_delay, attempt, retry_after);
    ++attempt;
    if (config.on_retry) {
      auto const delay_secs
        = std::chrono::duration_cast<std::chrono::seconds>(delay);
      config.on_retry(
        fmt::format("{}, attempt {}/{}, retrying after {}s", retry_reason,
                    attempt, config.max_retry_count + 1u, delay_secs.count()));
    }
    co_await folly::coro::sleep(delay);
  }
}

/// Custom deleter that ensures the session pool is destroyed on its event base
/// thread, which proxygen requires for safe cleanup.
struct SessionPoolDeleter {
  void operator()(proxygen::coro::HTTPCoroSessionPool* pool) const {
    if (not pool) {
      return;
    }
    auto& evb = *pool->getEventBase();
    evb.runImmediatelyOrRunInEventBaseThreadAndWait([pool] {
      delete pool;
    });
  }
};

using SessionPoolPtr
  = std::unique_ptr<proxygen::coro::HTTPCoroSessionPool, SessionPoolDeleter>;

auto make_session_pool(auto&&... args) -> SessionPoolPtr {
  return SessionPoolPtr{new proxygen::coro::HTTPCoroSessionPool{
    std::forward<decltype(args)>(args)...,
  }};
}

auto make_shared_session_pool(auto&&... args)
  -> std::shared_ptr<proxygen::coro::HTTPCoroSessionPool> {
  return std::shared_ptr<proxygen::coro::HTTPCoroSessionPool>{
    make_session_pool(std::forward<decltype(args)>(args)...),
  };
}

} // namespace

namespace {

/// Extracts the OpenSSL error code from a message that `ERR_error_string`
/// formatted, as in `error:4080003E:...`.
auto parse_openssl_error(std::string_view message) -> unsigned long {
  constexpr auto prefix = std::string_view{"error:"};
  auto const begin = message.find(prefix);
  if (begin == std::string_view::npos) {
    return 0;
  }
  auto const digits = message.substr(begin + prefix.size(), 8);
  auto code = 0ul;
  auto const [end, ec]
    = std::from_chars(digits.data(), digits.data() + digits.size(), code, 16);
  if (ec != std::errc{} or end != digits.data() + digits.size()) {
    return 0;
  }
  return code;
}

/// Finds the verification failure that a failed handshake reported.
auto find_verification_failure(folly::exception_wrapper const& error)
  -> Option<PeerVerificationFailure> {
  auto result = Option<PeerVerificationFailure>{};
  // Proxygen rethrows failed connections as plain `AsyncSocketException`s,
  // which keep the OpenSSL error only in their message.
  error.with_exception([&](folly::AsyncSocketException const& socket_error) {
    if (socket_error.getType() == folly::AsyncSocketException::SSL_ERROR) {
      result
        = peer_verification_failure(parse_openssl_error(socket_error.what()));
    }
  });
  error.with_exception([&](folly::SSLException const& ssl_error) {
    result = peer_verification_failure(ssl_error.getInternalSSLError());
  });
  error.with_exception(
    [&](proxygen::coro::HTTPCoroSessionPool::Exception const& pool_error) {
      result = find_verification_failure(pool_error.connectException);
    });
  return result;
}

} // namespace

auto describe_tls_verification_error(folly::exception_wrapper const& error,
                                     std::string_view host,
                                     Option<std::string_view> proxy_host)
  -> Option<std::string> {
  if (auto const* described = error.get_exception<TlsVerificationError>()) {
    return std::string{described->what()};
  }
  auto failure = find_verification_failure(error);
  if (not failure) {
    return None{};
  }
  if (failure->peer == TlsPeer::server) {
    return fmt::format("TLS certificate verification failed for `{}`: {}", host,
                       failure->reason);
  }
  if (proxy_host) {
    return fmt::format("TLS certificate verification failed for proxy `{}`: {}",
                       *proxy_host, failure->reason);
  }
  return fmt::format("TLS certificate verification failed for the proxy: {}",
                     failure->reason);
}

HttpClientTls::HttpClientTls(TlsConfig config)
  : config_{std::make_shared<TlsConfig const>(std::move(config))} {
}

auto HttpClientTls::make(TlsConfig config, std::string_view host,
                         diagnostic_handler& dh) -> failure_or<HttpClientTls> {
  auto result = HttpClientTls{std::move(config)};
  TRY(result.prepared_, result.config_->make_folly_ssl_context(dh, host, true));
  result.prepared_host_ = std::string{host};
  return result;
}

auto HttpClientTls::without_client_identity() const -> HttpClientTls {
  if (not config_ or (not config_->certfile and not config_->keyfile)) {
    return *this;
  }
  return HttpClientTls{config_->without_client_identity()};
}

auto HttpClientTls::context_for(std::string_view host) const
  -> Result<std::shared_ptr<folly::SSLContext>, std::string> {
  if (not config_) {
    return std::shared_ptr<folly::SSLContext>{};
  }
  if (prepared_ and host == prepared_host_) {
    return prepared_;
  }
  // A context verifies the certificate of exactly one host, so other hosts
  // need their own.
  auto dh = collecting_diagnostic_handler{};
  auto result = config_->make_folly_ssl_context(dh, host, true);
  if (result.is_error()) {
    auto diags = std::move(dh).collect();
    if (diags.empty()) {
      return Err{fmt::format("failed to set up TLS for `{}`", host)};
    }
    return Err{std::move(diags.front().message)};
  }
  return std::move(*result);
}

struct HttpPool::Impl {
  folly::Executor::KeepAlive<folly::IOExecutor> executor;
  folly::EventBase* evb = nullptr;
  proxygen::URL url;
  HttpPoolConfig config;
  /// TLS context that verifies the certificate of `url`'s host, or `nullptr`
  /// for plaintext connections.
  std::shared_ptr<folly::SSLContext const> ssl_context;
  /// The host of the HTTPS proxy that the session pool connects through.
  Option<std::string> proxy_host = None{};
  SessionPoolPtr pool;
  HttpRequestTargetForm target_form = HttpRequestTargetForm::origin;
  Option<proxy_url> proxy = None{};
  bool use_authenticated_proxy = false;
};

/// Names the certificate that failed verification in a failed attempt.
auto describe_tls_error(auto const& impl, folly::exception_wrapper const& error)
  -> Option<std::string> {
  return describe_tls_verification_error(
    error, impl.url.getHost(), impl.proxy_host.map([](std::string const& host) {
      return std::string_view{host};
    }));
}

auto make_conn_params(auto const& impl)
  -> proxygen::coro::HTTPCoroConnector::ConnectionParams {
  auto conn_params = proxygen::coro::HTTPClient::getConnParams(
    proxygen::coro::HTTPClient::SecureTransportImpl::NONE, impl.url.getHost());
  if (impl.ssl_context) {
    conn_params.serverName = impl.url.getHost();
    conn_params.sslContext = impl.ssl_context;
  }
  return conn_params;
}

auto make_session_params(HttpPoolConfig const& config)
  -> proxygen::coro::HTTPCoroConnector::SessionParams {
  auto session_params
    = proxygen::coro::HTTPCoroConnector::defaultSessionParams();
  if (config.max_concurrent_streams_per_connection > 0) {
    session_params.maxConcurrentOutgoingStreams
      = config.max_concurrent_streams_per_connection;
  }
  return session_params;
}

class HttpPoolRequest {
public:
  HttpPoolRequest() = default;
  explicit HttpPoolRequest(HttpProxyRequest request)
    : one_shot_{std::move(request)} {
    target_form_ = one_shot_->target_form();
    proxy_ = one_shot_->proxy();
  }
  HttpPoolRequest(
    proxygen::coro::HTTPCoroSession& session,
    proxygen::coro::HTTPCoroSession::RequestReservation reservation,
    HttpRequestTargetForm target_form, Option<proxy_url> proxy)
    : session_{session},
      reservation_{std::move(reservation)},
      target_form_{target_form},
      proxy_{proxy} {
  }
  auto target_form() const -> HttpRequestTargetForm {
    return target_form_;
  }
  auto proxy() const -> Option<proxy_url> {
    return proxy_;
  }
  auto send(proxygen::coro::HTTPSourceHolder source,
            proxygen::coro::HTTPSourceReader reader,
            std::chrono::milliseconds timeout) -> Task<void> {
    if (one_shot_) {
      co_return co_await one_shot_->send(std::move(source), std::move(reader),
                                         timeout);
    }
    TENZIR_ASSERT_ALWAYS(session_);
    co_await proxygen::coro::HTTPClient::request(&session_->get(),
                                                 std::move(reservation_),
                                                 std::move(source),
                                                 std::move(reader), timeout);
  }

private:
  Option<HttpProxyRequest> one_shot_ = None{};
  Option<Ref<proxygen::coro::HTTPCoroSession>> session_ = None{};
  proxygen::coro::HTTPCoroSession::RequestReservation reservation_;
  HttpRequestTargetForm target_form_ = HttpRequestTargetForm::origin;
  Option<proxy_url> proxy_ = None{};
};

auto get_session_with_reservation(auto const& impl) -> Task<HttpPoolRequest> {
  if (impl->use_authenticated_proxy) {
    auto conn_params = make_conn_params(*impl);
    auto sess_params = make_session_params(impl->config);
    auto request = co_await make_http_proxy_request(
      *impl->evb, impl->url.getHost(), impl->url.getPort(),
      std::move(conn_params), std::move(sess_params),
      impl->config.connection_timeout, impl->url.isSecure());
    co_return HttpPoolRequest{std::move(request)};
  }
  auto pooled = co_await impl->pool->getSessionWithReservation();
  TENZIR_ASSERT_ALWAYS(pooled.session);
  co_return HttpPoolRequest{*pooled.session, std::move(pooled.reservation),
                            impl->target_form, impl->proxy};
}

namespace {

/// Creates Proxygen's default TLS context for `host`, which trusts the CA
/// bundles of `http::ensure_default_ca_paths`.
auto make_default_ssl_context(std::string const& host, TlsPeer peer)
  -> Result<std::shared_ptr<folly::SSLContext const>, std::string> {
  auto const server = peer == TlsPeer::proxy ? fmt::format("proxy `{}`", host)
                                             : fmt::format("`{}`", host);
  try {
    http::ensure_default_ca_paths();
    // Proxygen loads the CA bundles here and throws if it cannot.
    auto ssl_context
      = proxygen::coro::HTTPClient::getConnParams(
          proxygen::coro::HTTPClient::SecureTransportImpl::TLS, host)
          .sslContext;
    if (not set_expected_peer_name(*ssl_context, host)) {
      return Err{fmt::format("failed to set up TLS for {}: invalid server name",
                             server)};
    }
    record_peer_verification(*ssl_context, peer);
    return ssl_context;
  } catch (std::exception const& error) {
    return Err{
      fmt::format("failed to set up TLS for {}: {}", server, error.what())};
  }
}

/// Creates the session pools of `impl` for its URL, directly or through
/// `proxy`.
auto make_session_pools(auto& impl, Option<proxy_url> proxy)
  -> Result<void, std::string> {
  auto const host = impl.url.getHost();
  auto const tls = impl.url.isSecure();
  auto conn_params = make_conn_params(impl);
  auto pool_params = proxygen::coro::HTTPCoroSessionPool::PoolParams{};
  pool_params.connectTimeout = impl.config.connection_timeout;
  auto session_params = make_session_params(impl.config);
  // When a proxy is configured and the target is not on the bypass list,
  // cleartext HTTP pools sessions to the proxy and sends absolute-form
  // requests. HTTPS targets reuse Proxygen's proxyPool support for CONNECT
  // tunnels, except authenticated CONNECT uses the one-shot helper below.
  if (not proxy) {
    impl.pool
      = make_session_pool(impl.evb, host, impl.url.getPort(), pool_params,
                          conn_params, session_params, true);
    return {};
  }
  impl.target_form
    = tls ? HttpRequestTargetForm::origin : HttpRequestTargetForm::absolute;
  if (not tls) {
    impl.proxy = *proxy;
  }
  if (tls and proxy->username) {
    impl.use_authenticated_proxy = true;
    return {};
  }
  // TLS-wrap the connection to the proxy itself when the proxy URL uses
  // `https://`. The CONNECT tunnel then carries the target's own TLS (when
  // `conn_params` requests it).
  auto proxy_conn_params = proxygen::coro::HTTPClient::getConnParams(
    proxygen::coro::HTTPClient::SecureTransportImpl::NONE, proxy->host);
  if (proxy->scheme == "https") {
    TRY(proxy_conn_params.sslContext,
        make_default_ssl_context(proxy->host, TlsPeer::proxy));
    proxy_conn_params.serverName = proxy->host;
    impl.proxy_host = proxy->host;
  }
  if (impl.target_form == HttpRequestTargetForm::absolute) {
    impl.pool = make_session_pool(
      impl.evb, proxy->host, proxy->port, pool_params, proxy_conn_params,
      proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
      /*allowNameLookup=*/true);
    return {};
  }
  auto proxy_pool = make_shared_session_pool(
    impl.evb, proxy->host, proxy->port, pool_params, proxy_conn_params,
    proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
    /*allowNameLookup=*/true);
  impl.pool = make_session_pool(impl.evb, host, impl.url.getPort(),
                                std::move(proxy_pool), pool_params, conn_params,
                                session_params,
                                /*observer=*/nullptr);
  return {};
}

/// Sets up a pool for `url`.
auto initialize(auto& impl, std::string const& url)
  -> Result<void, std::string> {
  impl.url = proxygen::URL{url};
  if (not impl.url.isValid() or not impl.url.hasHost()) {
    // The URL may contain credentials, so it stays out of the message.
    return Err{std::string{"invalid HTTP URL: expected a URL with a host"}};
  }
  auto const host = impl.url.getHost();
  auto const tls = impl.url.isSecure();
  if (tls) {
    auto ssl_context = impl.config.tls.context_for(host);
    if (ssl_context.is_err()) {
      return Err{fmt::format("failed to set up TLS for `{}`: {}", host,
                             std::move(ssl_context).unwrap_err())};
    }
    impl.ssl_context = std::move(ssl_context).unwrap();
    if (not impl.ssl_context) {
      TRY(impl.ssl_context, make_default_ssl_context(host, TlsPeer::server));
    }
  }
  auto proxy = proxy_for_target(tls ? "https" : "http", host);
  // Proxygen resolves the host of the server or proxy while creating the
  // session pool and throws when it cannot.
  auto const resolved_host = proxy ? proxy->host : host;
  try {
    return make_session_pools(impl, std::move(proxy));
  } catch (std::system_error const&) {
    // The resolver's message differs between platforms.
    return Err{fmt::format("failed to resolve `{}`", resolved_host)};
  } catch (std::exception const& error) {
    return Err{fmt::format("failed to set up a connection to `{}`: {}",
                           resolved_host, error.what())};
  }
}

} // namespace

HttpPool::HttpPool(std::shared_ptr<Impl> impl) : impl_{std::move(impl)} {
}

auto HttpPool::make(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                    std::string url, HttpPoolConfig config)
  -> Result<Box<HttpPool>, std::string> {
  auto impl = std::make_shared<Impl>();
  impl->executor = std::move(executor);
  impl->evb = impl->executor->getEventBase();
  impl->config = std::move(config);
  TRY(initialize(*impl, url));
  return Box<HttpPool>::from_non_null(
    std::unique_ptr<HttpPool>{new HttpPool{std::move(impl)}});
}

HttpPool::~HttpPool() = default;
HttpPool::HttpPool(HttpPool&&) noexcept = default;
auto HttpPool::operator=(HttpPool&&) noexcept -> HttpPool& = default;

auto HttpPool::request(proxygen::HTTPMethod method, std::string body,
                       std::vector<http::Header> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(method, None{}, std::move(body),
                             std::move(headers));
}

auto HttpPool::request(proxygen::HTTPMethod method, std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(method, std::move(body),
                             to_header_vector(std::move(headers)));
}

auto HttpPool::request(proxygen::HTTPMethod method, Option<std::string> path,
                       std::string body, std::vector<http::Header> headers)
  -> Task<Result<http::Response, std::string>> {
  auto make_headers
    = [headers = std::move(
         headers)]() -> Task<Result<std::vector<http::Header>, std::string>> {
    co_return headers;
  };
  co_return co_await request(method, std::move(path), std::move(body),
                             std::move(make_headers));
}

auto HttpPool::request(proxygen::HTTPMethod method, Option<std::string> path,
                       std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(method, std::move(path), std::move(body),
                             to_header_vector(std::move(headers)));
}

auto HttpPool::request(proxygen::HTTPMethod method, Option<std::string> path,
                       std::string body, HttpHeaderFactory make_headers)
  -> Task<Result<http::Response, std::string>> {
  auto p = path.unwrap_or_else([&]() {
    return impl_->url.makeRelativeURL();
  });
  co_return co_await co_withExecutor(
    impl_->evb,
    [](std::shared_ptr<Impl> impl, proxygen::HTTPMethod method,
       std::string path, std::string body, HttpHeaderFactory make_headers)
      -> Task<Result<http::Response, std::string>> {
      co_return co_await retry_request(
        impl->config,
        [&](folly::exception_wrapper const& error) {
          return describe_tls_error(*impl, error);
        },
        [&]() -> Task<proxygen::coro::HTTPClient::Response> {
          auto headers = co_await make_headers();
          if (headers.is_err()) {
            throw std::runtime_error{std::move(headers).unwrap_err()};
          }
          auto request_headers = std::move(headers).unwrap();
          auto session = co_await get_session_with_reservation(impl);
          auto* source
            = make_request_source(impl->url, path, method, request_headers,
                                  body, session.target_form(), session.proxy());
          auto resp = proxygen::coro::HTTPClient::Response{};
          co_await session.send(
            source, proxygen::coro::HTTPClient::makeDefaultReader(resp),
            impl->config.request_timeout);
          co_return resp;
        });
    }(impl_, method, std::move(p), std::move(body), std::move(make_headers)));
}

auto HttpPool::post(std::string body, std::vector<http::Header> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::POST, std::move(body),
                             std::move(headers));
}

auto HttpPool::post(std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await post(std::move(body),
                          to_header_vector(std::move(headers)));
}

auto HttpPool::post(std::string path, std::string body,
                    std::vector<http::Header> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::POST, std::move(path),
                             std::move(body), std::move(headers));
}

auto HttpPool::post(std::string path, std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await post(std::move(path), std::move(body),
                          to_header_vector(std::move(headers)));
}

auto HttpPool::post(std::string path, std::string body,
                    HttpHeaderFactory make_headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::POST, std::move(path),
                             std::move(body), std::move(make_headers));
}

auto HttpPool::get(std::string path, std::vector<http::Header> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::GET, std::move(path), "",
                             std::move(headers));
}

auto HttpPool::get(std::string path, std::map<std::string, std::string> headers)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await get(std::move(path), to_header_vector(std::move(headers)));
}

auto HttpPool::stream_get(std::string path, std::vector<http::Header> headers,
                          HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await stream_request(proxygen::HTTPMethod::GET, std::move(path),
                                    {}, std::move(headers),
                                    std::move(callbacks));
}

auto HttpPool::stream_request(proxygen::HTTPMethod method, std::string path,
                              std::string body,
                              std::vector<http::Header> headers,
                              HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  auto make_headers
    = [headers = std::move(
         headers)]() -> Task<Result<std::vector<http::Header>, std::string>> {
    co_return headers;
  };
  co_return co_await stream_request(method, std::move(path), std::move(body),
                                    std::move(make_headers),
                                    std::move(callbacks));
}

auto HttpPool::stream_request(proxygen::HTTPMethod method, std::string path,
                              std::string body, HttpHeaderFactory make_headers,
                              HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  auto stream_result = co_await async_try(co_withExecutor(
    impl_->evb,
    [](std::shared_ptr<Impl> impl, proxygen::HTTPMethod method,
       std::string path, std::string body, HttpHeaderFactory make_headers,
       HttpStreamCallbacks callbacks)
      -> Task<Result<http::Response, std::string>> {
      auto attempt = uint32_t{0};
      while (true) {
        auto body_started = false;
        auto body_cancelled = false;
        auto received_headers = false;
        auto retry_after = Option<std::chrono::seconds>{};
        auto retry_reason = std::string{};
        auto retryable_status = false;
        auto result = co_await async_try([&]() -> Task<http::Response> {
          auto headers = co_await make_headers();
          if (headers.is_err()) {
            throw std::runtime_error{std::move(headers).unwrap_err()};
          }
          auto request_headers = std::move(headers).unwrap();
          auto session = co_await get_session_with_reservation(impl);
          auto* source
            = make_request_source(impl->url, path, method, request_headers,
                                  body, session.target_form(), session.proxy());
          auto response = http::Response{};
          auto reader = proxygen::coro::HTTPSourceReader{};
          auto error_code = Option<proxygen::coro::HTTPErrorCode>{};
          auto error_message = std::string{};
          reader
            .onHeaders([&](std::unique_ptr<proxygen::HTTPMessage> headers,
                           bool is_final, bool) {
              if (not is_final) {
                return proxygen::coro::HTTPSourceReader::Continue;
              }
              response = http::to_http_response(*headers);
              received_headers = true;
              auto const status = response.status_code;
              retryable_status = attempt < impl->config.max_retry_count
                                 and http::is_retryable_http_status(status);
              if (retryable_status) {
                retry_reason = fmt::format("HTTP error {}", status);
                retry_after = http::parse_retry_after(
                  headers->getHeaders().getSingleOrEmpty("Retry-After"));
                return proxygen::coro::HTTPSourceReader::Cancel;
              }
              if (callbacks.on_headers) {
                callbacks.on_headers(response);
              }
              return proxygen::coro::HTTPSourceReader::Continue;
            })
            .onBodyAsync([&](proxygen::coro::BufQueue body,
                             bool) -> folly::coro::Task<bool> {
              if (body.empty() or not callbacks.on_body) {
                co_return proxygen::coro::HTTPSourceReader::Continue;
              }
              body_started = true;
              body_cancelled
                = co_await callbacks.on_body(body.move()->to<std::string>());
              co_return body_cancelled;
            })
            .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                         proxygen::coro::HTTPError err) {
              error_code = err.code;
              error_message = std::move(err.msg);
            });
          co_await session.send(source, std::move(reader),
                                impl->config.request_timeout);
          if (error_code and not retryable_status) {
            throw proxygen::coro::HTTPError{*error_code,
                                            std::move(error_message)};
          }
          co_return response;
        }());
        if (result.is_ok()) {
          if (not received_headers) {
            co_await folly::coro::co_safe_point;
            if (attempt >= impl->config.max_retry_count) {
              co_return Err{"request completed without a response"};
            }
            retry_reason = "connection error";
          } else if (not retryable_status) {
            co_return std::move(result).unwrap();
          }
        } else if (retryable_status) {
          // We cancelled the read before the body to retry a retryable status.
        } else {
          auto attempt_err = result.unwrap_err();
          // Retrying cannot fix a certificate that fails verification.
          if (auto error = describe_tls_error(*impl, attempt_err)) {
            co_return Err{std::move(*error)};
          }
          auto is_retryable = false;
          attempt_err.with_exception([&](folly::AsyncSocketException const&) {
            is_retryable = true;
          });
          attempt_err.with_exception([&](proxygen::coro::HTTPError const& err) {
            is_retryable = http::is_retryable_http_error(err.code);
          });
          attempt_err.with_exception(
            [&](proxygen::coro::HTTPCoroSessionPool::Exception const& err) {
              using Type = proxygen::coro::HTTPCoroSessionPool::Exception::Type;
              is_retryable = err.type == Type::Timeout;
            });
          if (body_cancelled or (body_started and not callbacks.on_retry)
              or not is_retryable or attempt >= impl->config.max_retry_count) {
            co_return Err{std::move(attempt_err).what().toStdString()};
          }
          retry_reason = "connection error";
        }
        if (callbacks.on_retry) {
          callbacks.on_retry();
        }
        auto delay = http::retry_delay_for_attempt(impl->config.retry_delay,
                                                   attempt, retry_after);
        ++attempt;
        if (impl->config.on_retry) {
          auto const delay_secs
            = std::chrono::duration_cast<std::chrono::seconds>(delay);
          impl->config.on_retry(fmt::format(
            "{}, attempt {}/{}, retrying after {}s", retry_reason, attempt,
            impl->config.max_retry_count + 1u, delay_secs.count()));
        }
        co_await folly::coro::sleep(delay);
      }
    }(impl_, method, std::move(path), std::move(body), std::move(make_headers),
      std::move(callbacks))));
  if (stream_result.is_err()) {
    co_return Err{std::move(stream_result).unwrap_err().what().toStdString()};
  }
  co_return std::move(stream_result).unwrap();
}

auto HttpPool::stream_post(std::string path, std::string body,
                           std::vector<http::Header> headers,
                           HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await stream_request(proxygen::HTTPMethod::POST, std::move(path),
                                    std::move(body), std::move(headers),
                                    std::move(callbacks));
}

auto HttpPool::stream_post(std::string path, std::string body,
                           HttpHeaderFactory make_headers,
                           HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await stream_request(proxygen::HTTPMethod::POST, std::move(path),
                                    std::move(body), std::move(make_headers),
                                    std::move(callbacks));
}

namespace {

auto http_request(folly::EventBase* evb, proxygen::HTTPMethod method,
                  std::string url, Option<std::string> body,
                  std::vector<http::Header> headers,
                  std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  co_return co_await co_withExecutor(
    evb,
    [](folly::EventBase* evb, proxygen::HTTPMethod method, std::string url,
       Option<std::string> body, std::vector<http::Header> headers,
       std::chrono::milliseconds timeout)
      -> Task<Result<http::Response, std::string>> {
      auto result = co_await async_try([&]() -> Task<http::Response> {
        auto url_parsed = proxygen::URL{url};
        if (not url_parsed.isValid() or not url_parsed.hasHost()) {
          throw std::runtime_error(fmt::format("invalid url: {}", url));
        }
        if (url_parsed.isSecure()) {
          http::ensure_default_ca_paths();
        }
        auto secure = url_parsed.isSecure()
                        ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                        : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
        auto conn_params = proxygen::coro::HTTPClient::getConnParams(
          secure, url_parsed.getHost());
        if (conn_params.sslContext) {
          if (not set_expected_peer_name(*conn_params.sslContext,
                                         url_parsed.getHost())) {
            throw std::runtime_error{fmt::format("invalid TLS server name `{}`",
                                                 url_parsed.getHost())};
          }
          record_peer_verification(*conn_params.sslContext);
        }
        auto sess_params
          = proxygen::coro::HTTPClient::getSessionParams(timeout);
        auto proxy_request = co_await make_http_proxy_request(
          *evb, url_parsed.getHost(), url_parsed.getPort(),
          std::move(conn_params), std::move(sess_params), timeout,
          url_parsed.isSecure());
        auto* source = make_request_source(
          url_parsed, url_parsed.makeRelativeURL(), method, headers,
          std::move(body), proxy_request.target_form(), proxy_request.proxy());
        auto resp = proxygen::coro::HTTPClient::Response{};
        co_await proxy_request.send(
          source, proxygen::coro::HTTPClient::makeDefaultReader(resp), timeout);
        if (not resp.headers) {
          // See `retry_request` for when a request completes without a
          // final response.
          co_await folly::coro::co_safe_point;
          throw std::runtime_error{"request completed without a response"};
        }
        co_return http::to_http_response(resp);
      }());
      if (result.is_err()) {
        if (auto error = describe_tls_verification_error(
              result.unwrap_err(), proxygen::URL{url}.getHost())) {
          co_return Err{std::move(*error)};
        }
        co_return Err{std::move(result).unwrap_err().what().toStdString()};
      }
      co_return std::move(result).unwrap();
    }(evb, method, std::move(url), std::move(body), std::move(headers),
      timeout));
}

} // namespace

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::vector<http::Header> headers,
               std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::POST, std::move(url),
                      std::move(body), std::move(headers), timeout);
}

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_post(evb, std::move(url), std::move(body),
                   to_header_vector(std::move(headers)), timeout);
}

auto http_get(folly::EventBase* evb, std::string url,
              std::vector<http::Header> headers,
              std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::GET, std::move(url), None{},
                      std::move(headers), timeout);
}

auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_get(evb, std::move(url), to_header_vector(std::move(headers)),
                  timeout);
}

auto http_delete(folly::EventBase* evb, std::string url,
                 std::vector<http::Header> headers,
                 std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::DELETE, std::move(url), None{},
                      std::move(headers), timeout);
}

auto http_delete(folly::EventBase* evb, std::string url,
                 std::map<std::string, std::string> headers,
                 std::chrono::milliseconds timeout)
  -> Task<Result<http::Response, std::string>> {
  return http_delete(evb, std::move(url), to_header_vector(std::move(headers)),
                     timeout);
}

} // namespace tenzir
