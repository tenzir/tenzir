//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/result.hpp>
#include <tenzir/detail/assert.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/http_proxy_connect.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/proxy_settings.hpp>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncSocketException.h>
#include <proxygen/lib/http/HTTPMessage.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <chrono>
#include <stdexcept>

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
                         Option<std::string> body)
  -> proxygen::coro::HTTPSource* {
  auto body_buf = std::unique_ptr<folly::IOBuf>{};
  if (body and not body->empty()) {
    body_buf = folly::IOBuf::fromString(std::move(*body));
  }
  auto* source = proxygen::coro::HTTPFixedSource::makeFixedRequest(
    std::move(path), method, std::move(body_buf));
  for (auto const& header : headers) {
    source->msg_->getHeaders().add(header.name, header.value);
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
  -> Task<Result<http::Response, std::string>> {
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
        co_return http::to_http_response(resp);
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
  bool use_authenticated_proxy = false;
};

auto make_conn_params(auto const& impl)
  -> proxygen::coro::HTTPCoroConnector::ConnectionParams {
  auto secure = impl.config.tls
                  ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
                  : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
  auto conn_params
    = proxygen::coro::HTTPClient::getConnParams(secure, impl.url.getHost());
  if (impl.config.ssl_context) {
    conn_params.sslContext = impl.config.ssl_context;
  }
  return conn_params;
}

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
  if (impl_->config.tls) {
    http::ensure_default_ca_paths();
  }
  auto conn_params = make_conn_params(*impl_);
  auto pool_params = proxygen::coro::HTTPCoroSessionPool::PoolParams{};
  pool_params.connectTimeout = impl_->config.connection_timeout;
  // When a proxy is configured and the target is not on the bypass
  // list, chain through Proxygen's built-in proxyPool support: TCP
  // connections to the proxy are pooled by `proxy_pool`, and the
  // target pool reuses CONNECT-tunnelled sessions on top of them.
  auto proxy = proxy_for_host(impl_->url.getHost());
  if (proxy) {
    auto const& ps = get_proxy_settings();
    TENZIR_ASSERT(ps.proxy_host and ps.proxy_port and ps.proxy_scheme);
    if (ps.proxy_username) {
      impl_->use_authenticated_proxy = true;
      return;
    }
    // TLS-wrap the connection to the proxy itself when the proxy
    // URL uses `https://`. The CONNECT tunnel then carries the
    // target's own TLS (when `conn_params` requests it).
    auto proxy_secure
      = *ps.proxy_scheme == "https"
          ? proxygen::coro::HTTPClient::SecureTransportImpl::TLS
          : proxygen::coro::HTTPClient::SecureTransportImpl::NONE;
    if (proxy_secure == proxygen::coro::HTTPClient::SecureTransportImpl::TLS) {
      http::ensure_default_ca_paths();
    }
    auto proxy_conn_params
      = proxygen::coro::HTTPClient::getConnParams(proxy_secure, *ps.proxy_host);
    auto proxy_pool = std::shared_ptr<proxygen::coro::HTTPCoroSessionPool>(
      new proxygen::coro::HTTPCoroSessionPool(
        impl_->evb, *ps.proxy_host, *ps.proxy_port, pool_params,
        proxy_conn_params,
        proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
        /*allowNameLookup=*/true),
      SessionPoolDeleter{});
    impl_->pool = SessionPoolPtr{
      std::make_unique<proxygen::coro::HTTPCoroSessionPool>(
        impl_->evb, impl_->url.getHost(), impl_->url.getPort(),
        std::move(proxy_pool), pool_params, conn_params,
        proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
        /*observer=*/nullptr)
        .release(),
    };
  } else {
    impl_->pool = SessionPoolPtr{
      std::make_unique<proxygen::coro::HTTPCoroSessionPool>(
        impl_->evb, impl_->url.getHost(), impl_->url.getPort(), pool_params,
        conn_params, proxygen::coro::HTTPCoroConnector::defaultSessionParams(),
        true)
        .release(),
    };
  }
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
  auto make_headers =
    [headers
     = std::move(headers)]() -> Result<std::vector<http::Header>, std::string> {
    return headers;
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
        impl->config, [&]() -> Task<proxygen::coro::HTTPClient::Response> {
          auto headers = make_headers();
          if (headers.is_err()) {
            throw std::runtime_error{std::move(headers).unwrap_err()};
          }
          auto request_headers = std::move(headers).unwrap();
          auto* session
            = static_cast<proxygen::coro::HTTPCoroSession*>(nullptr);
          auto reservation
            = proxygen::coro::HTTPCoroSession::RequestReservation{};
          auto holder = proxygen::coro::HTTPSessionContextPtr{};
          if (impl->use_authenticated_proxy) {
            auto conn_params = make_conn_params(*impl);
            auto sess_params
              = proxygen::coro::HTTPCoroConnector::defaultSessionParams();
            session = co_await connect_session_via_proxy_if_configured(
              impl->evb, impl->url.getHost(), impl->url.getPort(),
              std::move(conn_params), std::move(sess_params),
              impl->config.connection_timeout);
            holder = session->acquireKeepAlive();
            auto direct_reservation = session->reserveRequest();
            if (direct_reservation.hasException()) {
              co_yield folly::coro::co_error(
                std::move(direct_reservation.exception()));
            }
            reservation = std::move(*direct_reservation);
          } else {
            auto sr = co_await impl->pool->getSessionWithReservation();
            TENZIR_ASSERT_ALWAYS(sr.session);
            session = sr.session;
            reservation = std::move(sr.reservation);
          }
          SCOPE_EXIT {
            if (auto* s = holder.get()) {
              s->initiateDrain();
            }
          };
          auto* source = make_request_source(impl->url, path, method,
                                             request_headers, body);
          auto resp = proxygen::coro::HTTPClient::Response{};
          co_await proxygen::coro::HTTPClient::request(
            session, std::move(reservation), source,
            proxygen::coro::HTTPClient::makeDefaultReader(resp),
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

auto HttpPool::stream_request(proxygen::HTTPMethod method, std::string path,
                              std::string body,
                              std::vector<http::Header> headers,
                              HttpStreamCallbacks callbacks)
  -> Task<Result<http::Response, std::string>> {
  auto make_headers =
    [headers
     = std::move(headers)]() -> Result<std::vector<http::Header>, std::string> {
    return headers;
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
        auto retry_after = Option<std::chrono::seconds>{};
        auto retry_reason = std::string{};
        auto retryable_status = false;
        auto result = co_await async_try([&]() -> Task<http::Response> {
          auto headers = make_headers();
          if (headers.is_err()) {
            throw std::runtime_error{std::move(headers).unwrap_err()};
          }
          auto request_headers = std::move(headers).unwrap();
          auto* session
            = static_cast<proxygen::coro::HTTPCoroSession*>(nullptr);
          auto reservation
            = proxygen::coro::HTTPCoroSession::RequestReservation{};
          auto holder = proxygen::coro::HTTPSessionContextPtr{};
          if (impl->use_authenticated_proxy) {
            auto conn_params = make_conn_params(*impl);
            auto sess_params
              = proxygen::coro::HTTPCoroConnector::defaultSessionParams();
            session = co_await connect_session_via_proxy_if_configured(
              impl->evb, impl->url.getHost(), impl->url.getPort(),
              std::move(conn_params), std::move(sess_params),
              impl->config.connection_timeout);
            holder = session->acquireKeepAlive();
            auto direct_reservation = session->reserveRequest();
            if (direct_reservation.hasException()) {
              co_yield folly::coro::co_error(
                std::move(direct_reservation.exception()));
            }
            reservation = std::move(*direct_reservation);
          } else {
            auto sr = co_await impl->pool->getSessionWithReservation();
            TENZIR_ASSERT_ALWAYS(sr.session);
            session = sr.session;
            reservation = std::move(sr.reservation);
          }
          SCOPE_EXIT {
            if (auto* s = holder.get()) {
              s->initiateDrain();
            }
          };
          auto* source = make_request_source(impl->url, path, method,
                                             request_headers, body);
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
              co_return co_await callbacks.on_body(
                body.move()->to<std::string>());
            })
            .onError([&](proxygen::coro::HTTPSourceReader::ErrorContext,
                         proxygen::coro::HTTPError err) {
              error_code = err.code;
              error_message = std::move(err.msg);
            });
          co_await proxygen::coro::HTTPClient::request(
            session, std::move(reservation), source, std::move(reader),
            impl->config.request_timeout);
          if (error_code and not retryable_status) {
            throw proxygen::coro::HTTPError{*error_code,
                                            std::move(error_message)};
          }
          co_return response;
        }());
        if (result.is_ok()) {
          if (not retryable_status) {
            co_return std::move(result).unwrap();
          }
        } else if (retryable_status) {
          // We cancelled the read before the body to retry a retryable status.
        } else {
          auto attempt_err = result.unwrap_err();
          auto is_retryable = false;
          attempt_err.with_exception([&](folly::AsyncSocketException const&) {
            is_retryable = true;
          });
          attempt_err.with_exception([&](proxygen::coro::HTTPError const& err) {
            is_retryable = http::is_retryable_http_error(err.code);
          });
          if (body_started or not is_retryable
              or attempt >= impl->config.max_retry_count) {
            co_return Err{std::move(attempt_err).what().toStdString()};
          }
          retry_reason = "connection error";
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
        auto sess_params
          = proxygen::coro::HTTPClient::getSessionParams(timeout);
        auto* session = co_await connect_session_via_proxy_if_configured(
          evb, url_parsed.getHost(), url_parsed.getPort(),
          std::move(conn_params), std::move(sess_params), timeout);
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
          proxygen::coro::HTTPClient::makeDefaultReader(resp), timeout);
        co_return http::to_http_response(resp);
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

} // namespace tenzir
