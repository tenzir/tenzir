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
#include <tenzir/logger.hpp>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/coro/Sleep.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/portability/Time.h>
#include <proxygen/lib/http/HTTPMethod.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <algorithm>
#include <charconv>
#include <chrono>
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

using std::chrono::system_clock;

/// Parses an HTTP date and returns a `system_clock` time point.
auto parse_http_date(std::string_view value)
  -> Option<system_clock::time_point> {
  if (value.empty()) {
    return None{};
  }
  auto tm = std::tm{};
  auto parsed = std::string{value};
  if (::strptime(parsed.c_str(), "%a, %d %b %Y %H:%M:%S GMT", &tm) != nullptr
      or ::strptime(parsed.c_str(), "%a, %d-%b-%y %H:%M:%S GMT", &tm) != nullptr
      or ::strptime(parsed.c_str(), "%a %b %d %H:%M:%S %Y", &tm) != nullptr) {
    return system_clock::time_point{
      std::chrono::seconds{int64_t{timegm(&tm)}},
    };
  }
  return None{};
}

auto parse_retry_after(std::string_view value) -> Option<std::chrono::seconds> {
  if (value.empty()) {
    return None{};
  }
  // try parse a number of seconds
  auto seconds = uint64_t{};
  auto* begin = value.data();
  auto* end = value.data() + value.size();
  if (auto [ptr, ec] = std::from_chars(begin, end, seconds);
      ec == std::errc{} and ptr == end) {
    return std::chrono::seconds{seconds};
  }
  // try parse as HTTP date
  auto retry_at = parse_http_date(value);
  if (not retry_at) {
    return None{};
  }
  auto now = system_clock::now();
  return std::chrono::duration_cast<std::chrono::seconds>(
    retry_at <= now ? system_clock::duration::zero() : *retry_at - now);
}

auto retry_delay_for_attempt(HttpPoolConfig const& config, uint32_t attempt,
                             Option<std::chrono::seconds> retry_after)
  -> std::chrono::milliseconds {
  if (retry_after) {
    // explict Retry-After header
    return *retry_after;
  }
  // exponential backoff with upper bound
  auto delay = config.retry_delay;
  auto max_delay = config.retry_delay * 16;
  for (auto i = uint32_t{0}; i < attempt; ++i) {
    auto next_delay = delay * 2;
    if (next_delay <= delay) {
      return max_delay;
    }
    delay = next_delay;
    if (delay > max_delay) {
      return max_delay;
    }
  }
  return delay;
}

template <class F>
auto retry_request(HttpPoolConfig const& config, F&& f)
  -> Task<Result<HttpResponse, std::string>> {
  auto attempt = uint32_t{0};
  while (true) {
    auto retry_after = Option<std::chrono::seconds>{};
    auto attempt_res
      = co_await async_try([&]() -> Task<proxygen::coro::HTTPClient::Response> {
          co_return co_await f();
        }());
    if (attempt_res.is_ok()) {
      // got response
      proxygen::coro::HTTPClient::Response resp
        = std::move(attempt_res).unwrap();
      if (attempt >= config.max_retry_count
          or (not http::is_retryable_http_status(
            resp.headers->getStatusCode()))) {
        // not retryable
        co_return to_http_response(resp);
      }
      // retryable
      retry_after = parse_retry_after(
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
    }
    // will retry, compute delay
    auto delay = retry_delay_for_attempt(config, attempt, retry_after);
    ++attempt;
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
                  std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await co_withExecutor(
    evb,
    [](folly::EventBase* evb, proxygen::HTTPMethod method, std::string url,
       Option<std::string> body, std::map<std::string, std::string> headers,
       std::chrono::milliseconds timeout)
      -> Task<Result<HttpResponse, std::string>> {
      auto result = co_await async_try([&]() -> Task<HttpResponse> {
        auto url_parsed = proxygen::URL{url};
        if (not url_parsed.isValid() or not url_parsed.hasHost()) {
          throw std::runtime_error(fmt::format("invalid url: {}", url));
        }
        if (url_parsed.isSecure()) {
          ensure_http_default_ca_paths();
        }
        auto* session = co_await proxygen::coro::HTTPClient::getHTTPSession(
          evb, url_parsed.getHost(), url_parsed.getPort(),
          url_parsed.isSecure(), false, timeout, timeout);
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
  return http_request(evb, proxygen::HTTPMethod::GET, std::move(url), None{},
                      std::move(headers), timeout);
}

} // namespace tenzir
