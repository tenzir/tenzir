//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/result.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/logger.hpp>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/Retry.h>
#include <folly/io/async/AsyncSocketException.h>
#include <proxygen/lib/http/coro/HTTPError.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <cctype>
#include <filesystem>
#include <mutex>

namespace tenzir {

namespace http {

auto is_retryable_http_error(proxygen::coro::HTTPErrorCode code) -> bool {
  using code_t = proxygen::coro::HTTPErrorCode;
  return code == code_t::TRANSPORT_EOF or code == code_t::TRANSPORT_READ_ERROR
         or code == code_t::TRANSPORT_WRITE_ERROR
         or code == code_t::READ_TIMEOUT or code == code_t::WRITE_TIMEOUT;
}

auto is_retryable_http_status(uint16_t status_code) -> bool {
  return status_code == 429 or (status_code >= 500 and status_code <= 599);
}

retryable_http_response::retryable_http_response(
  uint16_t status_code,
  std::vector<std::pair<std::string, std::string>> headers, std::string body)
  : std::runtime_error{fmt::format("retryable HTTP status {}", status_code)},
    status_code{status_code},
    headers{std::move(headers)},
    body{std::move(body)} {
}

} // namespace http

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
auto make_request_source(proxygen::URL const& url, proxygen::HTTPMethod method,
                         std::map<std::string, std::string> headers,
                         std::optional<std::string> body)
  -> proxygen::coro::HTTPSource* {
  auto body_buf = std::unique_ptr<folly::IOBuf>{};
  if (body and not body->empty()) {
    body_buf = folly::IOBuf::fromString(std::move(*body));
  }
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedRequest(
    url.makeRelativeURL(), method, std::move(body_buf));
  for (auto& [name, value] : headers) {
    source->msg_->getHeaders().add(name, std::move(value));
  }
  if (not source->msg_->getHeaders().exists(proxygen::HTTP_HEADER_HOST)) {
    source->msg_->getHeaders().add(proxygen::HTTP_HEADER_HOST,
                                   url.getHostAndPortOmitDefault());
  }
  source->msg_->setWantsKeepalive(true);
  source->msg_->setSecure(url.isSecure());
  return source;
}

auto with_target(proxygen::URL const& base, std::optional<std::string> target)
  -> Result<proxygen::URL, std::string> {
  if (not target) {
    return base;
  }
  if (target->empty() or target->front() != '/'
      or target->find('#') != std::string::npos) {
    return Err{fmt::format("invalid request target: {}", *target)};
  }
  auto path = std::string_view{*target};
  auto query = std::string_view{};
  if (auto pos = target->find('?'); pos != std::string::npos) {
    path = std::string_view{*target}.substr(0, pos);
    query = std::string_view{*target}.substr(pos + 1);
  }
  return proxygen::URL{
    base.getScheme(),  base.getHost(),     base.getPort(),
    std::string{path}, std::string{query},
  };
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
  impl_->config = config;
  if (not impl_->url.isValid() or not impl_->url.hasHost()) {
    throw std::runtime_error(fmt::format("invalid url: {}", url));
  }
  auto secure = config.tls
                  ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                  : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
  if (config.tls) {
    ensure_http_default_ca_paths();
  }
  auto conn_params
    = proxygen::coro::HTTPClient::getConnParams(secure, impl_->url.getHost());
  if (config.ssl_context) {
    conn_params.sslContext = config.ssl_context;
  }
  auto pool_params = proxygen::coro::HTTPCoroSessionPool::PoolParams{};
  pool_params.connectTimeout = config.connection_timeout;
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
    new HttpPool{std::move(executor), std::move(url), config},
  });
}

HttpPool::~HttpPool() = default;
HttpPool::HttpPool(HttpPool&&) noexcept = default;
auto HttpPool::operator=(HttpPool&&) noexcept -> HttpPool& = default;

auto HttpPool::request(std::string method, std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request(std::move(method), std::nullopt, std::move(body),
                             std::move(headers));
}

auto HttpPool::request(std::string method, std::string target, std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request(std::move(method),
                             std::optional<std::string>{std::move(target)},
                             std::move(body), std::move(headers));
}

auto HttpPool::request(std::string method, std::optional<std::string> target,
                       std::string body,
                       std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await co_withExecutor(
    impl_->evb,
    [](std::shared_ptr<Impl> impl, std::string method,
       std::optional<std::string> target, std::string body,
       std::map<std::string, std::string> headers)
      -> Task<Result<HttpResponse, std::string>> {
      std::ranges::transform(method, method.begin(), [](unsigned char c) {
        return static_cast<char>(std::toupper(c));
      });
      auto method_parsed = proxygen::stringToMethod(method);
      if (not method_parsed) {
        co_return Err{fmt::format("invalid http method: {}", method)};
      }
      CO_TRY(auto url, with_target(impl->url, std::move(target)));
      auto emitted_response = false;
      auto result = co_await async_try([&]() -> Task<HttpResponse> {
        auto response = HttpResponse{};
        co_await folly::coro::retryWithExponentialBackoff(
          impl->config.max_retry_count, impl->config.retry_delay,
          impl->config.retry_delay * 5, 0.0,
          [&]() -> Task<void> {
            emitted_response = false;
            auto sr = co_await impl->pool->getSessionWithReservation();
            TENZIR_ASSERT_ALWAYS(sr.session);
            auto attempt_headers = headers;
            auto attempt_body = body;
            auto* source = make_request_source(url, *method_parsed,
                                               std::move(attempt_headers),
                                               std::move(attempt_body));
            auto resp = proxygen::coro::HTTPClient::Response{};
            co_await proxygen::coro::HTTPClient::request(
              sr.session, std::move(sr.reservation), source,
              proxygen::coro::HTTPClient::makeDefaultReader(resp),
              impl->config.request_timeout);
            if (not resp.headers
                or http::is_retryable_http_status(
                  resp.headers->getStatusCode())) {
              auto headers = std::vector<std::pair<std::string, std::string>>{};
              if (resp.headers) {
                resp.headers->getHeaders().forEach(
                  [&](std::string const& name, std::string const& value) {
                    headers.emplace_back(name, value);
                  });
              }
              auto response_body = std::string{};
              if (not resp.body.empty()) {
                response_body = resp.body.move()->to<std::string>();
              }
              throw http::retryable_http_response{
                resp.headers ? resp.headers->getStatusCode() : uint16_t{0},
                std::move(headers), std::move(response_body)};
            }
            emitted_response = true;
            response = to_http_response(resp);
          },
          [&](folly::exception_wrapper const& ew) {
            if (emitted_response) {
              return false;
            }
            if (ew.is_compatible_with<folly::AsyncSocketException>()) {
              return true;
            }
            if (ew.is_compatible_with<http::retryable_http_response>()) {
              return true;
            }
            auto is_retryable = false;
            ew.with_exception([&](proxygen::coro::HTTPError const& err) {
              is_retryable = http::is_retryable_http_error(err.code);
            });
            return is_retryable;
          });
        co_return response;
      }());
      if (result.is_err()) {
        auto retryable_response = Option<HttpResponse>{};
        result.unwrap_err().with_exception(
          [&](http::retryable_http_response const& err) {
            retryable_response = HttpResponse{
              .status_code = err.status_code,
              .headers = std::map<std::string, std::string>{err.headers.begin(),
                                                            err.headers.end()},
              .body = err.body,
            };
          });
        if (retryable_response) {
          co_return std::move(*retryable_response);
        }
        co_return Err{std::move(result).unwrap_err().what().toStdString()};
      }
      co_return std::move(result).unwrap();
    }(impl_, std::move(method), std::move(target), std::move(body),
      std::move(headers)));
}

auto HttpPool::post(std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request("POST", std::move(body), std::move(headers));
}

auto HttpPool::post(std::string target, std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await request("POST", std::move(target), std::move(body),
                             std::move(headers));
}

namespace {

auto http_request(folly::EventBase* evb, proxygen::HTTPMethod method,
                  std::string url, std::optional<std::string> body,
                  std::map<std::string, std::string> headers,
                  std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await co_withExecutor(
    evb,
    [](folly::EventBase* evb, proxygen::HTTPMethod method, std::string url,
       std::optional<std::string> body,
       std::map<std::string, std::string> headers,
       std::chrono::milliseconds timeout)
      -> Task<Result<HttpResponse, std::string>> {
      auto result = co_await async_try([&]() -> Task<HttpResponse> {
        auto parsed = proxygen::URL{url};
        if (not parsed.isValid() or not parsed.hasHost()) {
          throw std::runtime_error(fmt::format("invalid url: {}", url));
        }
        if (parsed.isSecure()) {
          ensure_http_default_ca_paths();
        }
        auto* session = co_await proxygen::coro::HTTPClient::getHTTPSession(
          evb, parsed.getHost(), parsed.getPort(), parsed.isSecure(), false,
          timeout, timeout);
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
        auto* source = make_request_source(parsed, method, std::move(headers),
                                           std::move(body));
        auto resp = proxygen::coro::HTTPClient::Response{};
        co_await proxygen::coro::HTTPClient::request(
          session, std::move(*reservation), source,
          proxygen::coro::HTTPClient::makeDefaultReader(resp), timeout);
        co_return to_http_response(resp);
      }());
      if (result.is_err()) {
        co_return Err{std::move(result).unwrap_err().what().toStdString()};
      }
      co_return std::move(result).unwrap();
    }(evb, method, std::move(url), std::move(body), std::move(headers),
      timeout));
}

} // namespace

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::POST, std::move(url),
                      std::move(body), std::move(headers), timeout);
}

auto http_get(folly::EventBase* evb, std::string url,
              std::map<std::string, std::string> headers,
              std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  return http_request(evb, proxygen::HTTPMethod::GET, std::move(url),
                      std::nullopt, std::move(headers), timeout);
}

} // namespace tenzir
