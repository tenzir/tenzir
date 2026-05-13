//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/result.hpp>
#include <tenzir/concept/parseable/tenzir/endpoint.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/logger.hpp>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/SocketAddress.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncSocketException.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroConnector.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <charconv>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <system_error>

namespace tenzir {

auto ensure_http_default_ca_paths() -> void {
  static std::once_flag flag;
  std::call_once(flag, [] {
    auto ca_paths = std::vector<std::string>{};
    for (auto const* path : {
           "/etc/ssl/certs/ca-certificates.crt",
           "/etc/pki/tls/cert.pem",
           "/etc/ssl/cert.pem",
         }) {
      if (std::filesystem::exists(path)) {
        ca_paths.emplace_back(path);
      }
    }
    // If none is found, maybe we should warn here,
    // but only if node config tls and operator args were not set.
    if (not ca_paths.empty()) {
      proxygen::coro::HTTPClient::setDefaultCAPaths(std::move(ca_paths));
    }
  });
}

namespace {

auto to_http_response(proxygen::coro::HTTPClient::Response& resp)
  -> HttpResponse {
  auto result = HttpResponse{};
  TENZIR_ASSERT_ALWAYS(resp.headers);
  result.status_code = resp.headers->getStatusCode();
  resp.headers->getHeaders().forEach(
    [&](std::string const& name, std::string const& value) {
      result.headers[name] = value;
    });
  if (not resp.body.empty()) {
    result.body = resp.body.move()->to<std::string>();
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
                         std::map<std::string, std::string> const& headers,
                         Option<std::string> body)
  -> proxygen::coro::HTTPSource* {
  auto body_buf = std::unique_ptr<folly::IOBuf>{};
  if (body and not body->empty()) {
    body_buf = folly::IOBuf::fromString(std::move(*body));
  }
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedRequest(
    std::move(path), method, std::move(body_buf));
  for (auto const& [name, value] : headers) {
    source->msg_->getHeaders().add(name, value);
  }
  if (not source->msg_->getHeaders().exists(proxygen::HTTP_HEADER_HOST)) {
    source->msg_->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                                   host_url.getHostAndPortOmitDefault());
  }
  source->msg_->setWantsKeepalive(true);
  source->msg_->setSecure(host_url.isSecure());
  return source;
}

template <class F>
auto retry_request(HttpPoolConfig const& config, F&& f)
  -> Task<Result<HttpResponse, std::string>> {
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
      auto const status = resp.headers->getStatusCode();
      if (attempt >= config.max_retry_count
          or not http::is_retryable_http_status(status)) {
        // not retryable
        co_return to_http_response(resp);
      }
      // retryable HTTP status
      retry_reason = fmt::format("HTTP error {}", status);
      retry_after = http::parse_retry_after(
        resp.headers->getHeaders().getSingleOrEmpty("Retry-After"));
    } else {
      // transport error
      auto attempt_err = attempt_res.unwrap_err();
      auto is_retryable = false;
      attempt_err.with_exception([&](folly::AsyncSocketException const&) {
        is_retryable = true;
      });
      attempt_err.with_exception([&](proxygen::coro::HTTPError const& err) {
        is_retryable = http::is_retryable_http_error(err.code);
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

auto host_matches_no_proxy(std::string_view host, std::string_view pattern)
  -> bool {
  pattern = detail::trim(pattern);
  if (pattern.empty()) {
    return false;
  }
  if (pattern == "*") {
    return true;
  }
  auto parsed = endpoint{};
  if (parsers::endpoint(pattern, parsed)) {
    pattern = parsed.host;
  }
  if (host == pattern) {
    return true;
  }
  if (pattern.starts_with('.')) {
    pattern.remove_prefix(1);
  }
  return host.size() > pattern.size() and host.ends_with(pattern)
         and host[host.size() - pattern.size() - 1] == '.';
}

auto bypass_proxy(std::string_view host) -> bool {
  auto const* no_proxy = std::getenv("NO_PROXY");
  if (not no_proxy) {
    no_proxy = std::getenv("no_proxy");
  }
  if (not no_proxy) {
    return false;
  }
  for (auto pattern : detail::split(no_proxy, ",")) {
    if (host_matches_no_proxy(host, pattern)) {
      return true;
    }
  }
  return false;
}

auto https_proxy_for(proxygen::URL const& url) -> std::optional<proxygen::URL> {
  if (not url.isSecure() or bypass_proxy(url.getHost())) {
    return std::nullopt;
  }
  auto const* proxy = std::getenv("HTTPS_PROXY");
  if (not proxy) {
    proxy = std::getenv("https_proxy");
  }
  if (not proxy or std::string_view{proxy}.empty()) {
    return std::nullopt;
  }
  auto parsed = proxygen::URL{proxy};
  if (not parsed.isValid() or not parsed.hasHost()) {
    throw std::runtime_error(fmt::format("invalid HTTPS proxy URL: {}", proxy));
  }
  return parsed;
}

auto https_proxy_string_for(proxygen::URL const& url)
  -> std::optional<std::string> {
  auto proxy = https_proxy_for(url);
  if (not proxy) {
    return std::nullopt;
  }
  return std::string{proxy->getUrl()};
}

auto curl_http_request(proxygen::HTTPMethod method, std::string const& url,
                       Option<std::string> const& body,
                       std::map<std::string, std::string> const& headers,
                       HttpPoolConfig const& config,
                       std::optional<std::string> const& proxy)
  -> Result<HttpResponse, std::string> {
  auto easy = curl::easy{};
  auto response_body = std::string{};
  auto response_headers = std::map<std::string, std::string>{};
  auto write_callback = [&](std::span<std::byte const> data) {
    response_body.append(reinterpret_cast<char const*>(data.data()),
                         data.size());
  };
  if (auto code = easy.set(write_callback); code != curl::easy::code::ok) {
    return Err{fmt::format("failed to configure HTTP response body: {}",
                           to_string(code))};
  }
  auto header_callback = [&](std::span<std::byte const> data) {
    auto header = std::string_view{reinterpret_cast<char const*>(data.data()),
                                   data.size()};
    header = detail::trim(header);
    if (header.empty()) {
      return;
    }
    if (header.starts_with("HTTP/")) {
      response_headers.clear();
      return;
    }
    auto colon = header.find(':');
    if (colon == std::string_view::npos) {
      return;
    }
    auto [name, value] = detail::split_once(header, ":");
    name = detail::trim(name);
    value = detail::trim(value);
    if (not name.empty()) {
      response_headers[std::string{name}] = std::string{value};
    }
  };
  if (auto code = easy.set_header_callback(header_callback);
      code != curl::easy::code::ok) {
    return Err{fmt::format("failed to configure HTTP response headers: {}",
                           to_string(code))};
  }
  if (auto code = easy.set(CURLOPT_URL, url); code != curl::easy::code::ok) {
    return Err{
      fmt::format("failed to configure HTTP URL: {}", to_string(code))};
  }
  if (method == proxygen::HTTPMethod::POST) {
    if (auto code = easy.set(CURLOPT_POST, 1); code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure HTTP method: {}", to_string(code))};
    }
    auto const& request_body = body ? *body : std::string{};
    if (auto code = easy.set(CURLOPT_POSTFIELDS, request_body);
        code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure HTTP body: {}", to_string(code))};
    }
    auto body_size = static_cast<long>(request_body.size());
    if (auto code = easy.set_postfieldsize(body_size);
        code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure HTTP body size: {}", to_string(code))};
    }
  }
  for (auto const& [name, value] : headers) {
    if (auto code = easy.set_http_header(name, value);
        code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure HTTP header: {}", to_string(code))};
    }
  }
  if (proxy) {
    if (auto code = easy.set(CURLOPT_PROXY, *proxy);
        code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure HTTPS proxy: {}", to_string(code))};
    }
  }
  if (config.ca_info) {
    if (auto code = easy.set(CURLOPT_CAINFO, *config.ca_info);
        code != curl::easy::code::ok) {
      return Err{
        fmt::format("failed to configure TLS CA: {}", to_string(code))};
    }
  }
  if (config.skip_peer_verification) {
    static_cast<void>(easy.set(CURLOPT_SSL_VERIFYPEER, 0));
    static_cast<void>(easy.set(CURLOPT_SSL_VERIFYHOST, 0));
  }
  static_cast<void>(easy.set(
    CURLOPT_TIMEOUT_MS, static_cast<long>(config.request_timeout.count())));
  static_cast<void>(
    easy.set(CURLOPT_CONNECTTIMEOUT_MS,
             static_cast<long>(config.connection_timeout.count())));
  if (auto code = easy.perform(); code != curl::easy::code::ok) {
    return Err{fmt::format("curl error: {}", to_string(code))};
  }
  auto [code, status] = easy.get<curl::easy::info::response_code>();
  if (code != curl::easy::code::ok) {
    return Err{
      fmt::format("failed to read HTTP response code: {}", to_string(code))};
  }
  return HttpResponse{.status_code = static_cast<uint16_t>(status),
                      .headers = std::move(response_headers),
                      .body = std::move(response_body)};
}

auto make_session_params(std::chrono::milliseconds timeout)
  -> proxygen::coro::HTTPCoroConnector::SessionParams {
  auto params = proxygen::coro::HTTPClient::getSessionParams(timeout);
  return params;
}

auto make_connection_params(proxygen::URL const& url,
                            HttpPoolConfig const& config)
  -> proxygen::coro::HTTPCoroConnector::ConnectionParams {
  auto params = proxygen::coro::HTTPCoroConnector::defaultConnectionParams();
  params.serverName = url.getHost();
  if (url.isSecure()) {
    if (config.ssl_context) {
      params.sslContext = config.ssl_context;
    } else {
      auto tls_params = proxygen::coro::HTTPCoroConnector::TLSParams{};
      tls_params.caPaths = proxygen::coro::HTTPClient::getDefaultCAPaths();
      params.sslContext
        = proxygen::coro::HTTPCoroConnector::makeSSLContext(tls_params);
    }
  }
  return params;
}

auto connect_direct(folly::EventBase* evb, proxygen::URL const& url,
                    HttpPoolConfig const& config)
  -> Task<proxygen::coro::HTTPCoroSession*> {
  auto address = folly::SocketAddress{};
  address.setFromHostPort(url.getHost(), url.getPort());
  auto conn_params = make_connection_params(url, config);
  auto session_params = make_session_params(config.request_timeout);
  co_return co_await proxygen::coro::HTTPCoroConnector::connect(
    evb, std::move(address), config.connection_timeout, conn_params,
    session_params);
}

/// Custom deleter that ensures the session pool is destroyed on its event base
/// thread, which proxygen requires for safe cleanup.
struct SessionPoolDeleter {
  void operator()(proxygen::coro::HTTPCoroSessionPool* pool) const {
    if (not pool) {
      return;
    }
    auto* evb = pool->getEventBase();
    evb->runImmediatelyOrRunInEventBaseThreadAndWait([pool] {
      delete pool;
    });
  }
};

using SessionPoolPtr
  = std::unique_ptr<proxygen::coro::HTTPCoroSessionPool, SessionPoolDeleter>;

} // namespace

struct HttpPool::Impl {
  folly::Executor::KeepAlive<folly::IOExecutor> executor;
  folly::EventBase* evb = nullptr;
  proxygen::URL url;
  HttpPoolConfig config;
  SessionPoolPtr pool;
};

HttpPool::HttpPool(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                   std::string url, HttpPoolConfig config)
  : impl_{std::make_shared<Impl>()} {
  impl_->executor = std::move(executor);
  impl_->evb = impl_->executor->getEventBase();
  impl_->url = proxygen::URL{url};
  impl_->config = std::move(config);
  if (not impl_->url.isValid() or not impl_->url.hasHost()) {
    throw std::runtime_error(fmt::format("invalid url: {}", url));
  }
  auto secure = impl_->config.tls
                  ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                  : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
  if (impl_->config.tls) {
    ensure_http_default_ca_paths();
  }
  auto conn_params
    = proxygen::coro::HTTPClient::getConnParams(secure, impl_->url.getHost());
  if (impl_->config.ssl_context) {
    conn_params.sslContext = impl_->config.ssl_context;
  }
  auto pool_params = proxygen::coro::HTTPCoroSessionPool::PoolParams{};
  pool_params.connectTimeout = impl_->config.connection_timeout;
  impl_->pool = SessionPoolPtr{
    std::make_unique<proxygen::coro::HTTPCoroSessionPool>(
      impl_->evb, impl_->url.getHost(), impl_->url.getPort(), pool_params,
      conn_params, proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
      true)
      .release(),
  };
}

auto HttpPool::make(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                    std::string url, HttpPoolConfig config) -> Box<HttpPool> {
  return Box<HttpPool>::from_non_null(std::unique_ptr<HttpPool>{
    new HttpPool{std::move(executor), std::move(url), std::move(config)},
  });
}

HttpPool::~HttpPool() = default;
HttpPool::HttpPool(HttpPool&&) noexcept = default;
auto HttpPool::operator=(HttpPool&&) noexcept -> HttpPool& = default;

auto HttpPool::request(proxygen::HTTPMethod method, std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request(method, None{}, std::move(body),
                             std::move(headers));
}

auto HttpPool::request(proxygen::HTTPMethod method, Option<std::string> path,
                       std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  auto p = path.unwrap_or_else([&]() {
    return impl_->url.makeRelativeURL();
  });
  co_return co_await co_withExecutor(
    impl_->evb,
    [](std::shared_ptr<Impl> impl, proxygen::HTTPMethod method,
       std::string path, std::string body,
       std::map<std::string, std::string> headers)
      -> Task<Result<HttpResponse, std::string>> {
      co_return co_await retry_request(
        impl->config, [&]() -> Task<proxygen::coro::HTTPClient::Response> {
          auto sr = co_await impl->pool->getSessionWithReservation();
          TENZIR_ASSERT_ALWAYS(sr.session);
          auto* source
            = make_request_source(impl->url, path, method, headers, body);
          auto resp = proxygen::coro::HTTPClient::Response{};
          co_await proxygen::coro::HTTPClient::request(
            sr.session, std::move(sr.reservation), source,
            proxygen::coro::HTTPClient::makeDefaultReader(resp),
            impl->config.request_timeout);
          co_return resp;
        });
    }(impl_, method, std::move(p), std::move(body), std::move(headers)));
}

auto HttpPool::post(std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::POST, std::move(body),
                             std::move(headers));
}

auto HttpPool::post(std::string path, std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request(proxygen::HTTPMethod::POST, std::move(path),
                             std::move(body), std::move(headers));
}

namespace {

auto http_request(folly::EventBase* evb, proxygen::HTTPMethod method,
                  std::string url, Option<std::string> body,
                  std::map<std::string, std::string> headers,
                  HttpPoolConfig config)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await co_withExecutor(
    evb,
    [](folly::EventBase* evb, proxygen::HTTPMethod method, std::string url,
       Option<std::string> body, std::map<std::string, std::string> headers,
       HttpPoolConfig config) -> Task<Result<HttpResponse, std::string>> {
      auto result = co_await async_try([&]() -> Task<HttpResponse> {
        auto url_parsed = proxygen::URL{url};
        if (not url_parsed.isValid() or not url_parsed.hasHost()) {
          throw std::runtime_error(fmt::format("invalid url: {}", url));
        }
        if (url_parsed.isSecure()) {
          ensure_http_default_ca_paths();
        }
        auto proxy = https_proxy_string_for(url_parsed);
        if (proxy or config.ca_info or config.skip_peer_verification) {
          auto response
            = curl_http_request(method, url, body, headers, config, proxy);
          if (response.is_err()) {
            throw std::runtime_error{std::move(response).unwrap_err()};
          }
          co_return std::move(response).unwrap();
        }
        auto* session = co_await connect_direct(evb, url_parsed, config);
        auto holder = session->acquireKeepAlive();
        SCOPE_EXIT {
          if (auto* s = holder.get()) {
            s->initiateDrain();
          }
        };
        auto reservation = session->reserveRequest();
        if (reservation.hasException()) {
          co_yield folly::coro::co_error(std::move(reservation.exception()));
        }
        auto* source
          = make_request_source(url_parsed, url_parsed.makeRelativeURL(),
                                method, headers, std::move(body));
        auto resp = proxygen::coro::HTTPClient::Response{};
        co_await proxygen::coro::HTTPClient::request(
          session, std::move(*reservation), source,
          proxygen::coro::HTTPClient::makeDefaultReader(resp),
          config.request_timeout);
        co_return to_http_response(resp);
      }());
      if (result.is_err()) {
        co_return Err{std::move(result).unwrap_err().what().toStdString()};
      }
      co_return std::move(result).unwrap();
    }(evb, method, std::move(url), std::move(body), std::move(headers),
                              std::move(config)));
}

} // namespace

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  auto config
    = HttpPoolConfig{.request_timeout = timeout, .connection_timeout = timeout};
  return http_post(evb, std::move(url), std::move(body), std::move(headers),
                   std::move(config));
}

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               HttpPoolConfig config)
  -> Task<Result<HttpResponse, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::POST, std::move(url),
                      std::move(body), std::move(headers), std::move(config));
}

auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  auto config
    = HttpPoolConfig{.request_timeout = timeout, .connection_timeout = timeout};
  return http_get(evb, std::move(url), std::move(headers), std::move(config));
}

auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers, HttpPoolConfig config)
  -> Task<Result<HttpResponse, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::GET, std::move(url), None{},
                      std::move(headers), std::move(config));
}

} // namespace tenzir
