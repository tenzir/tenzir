//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/result.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/logger.hpp>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <proxygen/lib/http/coro/HTTPFixedSource.h>
#include <proxygen/lib/http/coro/client/HTTPClient.h>
#include <proxygen/lib/http/coro/client/HTTPCoroSessionPool.h>
#include <proxygen/lib/utils/URL.h>

#include <filesystem>
#include <mutex>

namespace tenzir {

namespace {

/// Scans well-known system paths for CA certificate bundles and configures
/// proxygen's default CA paths accordingly.
auto discover_and_set_ca_paths() -> void {
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
    if (ca_paths.empty()) {
      TENZIR_WARN("no CA certificate bundle found at well-known paths");
    }
    proxygen::coro::HTTPClient::setDefaultCAPaths(std::move(ca_paths));
  });
}

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
    discover_and_set_ca_paths();
  }
  auto conn_params
    = proxygen::coro::HTTPClient::getConnParams(secure, impl_->url.getHost());
  impl_->pool = SessionPoolPtr{
    std::make_unique<proxygen::coro::HTTPCoroSessionPool>(
      impl_->evb, impl_->url.getHost(), impl_->url.getPort(),
      proxygen::coro::HTTPCoroSessionPool::PoolParams{}, conn_params,
      proxygen::coro::HTTPCoroConnector::defaultSessionParams(), true)
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

auto HttpPool::post(std::string body,
                    std::map<std::string, std::string> headers)
  -> Task<Result<HttpResponse, std::string>> {
  co_return co_await co_withExecutor(
    impl_->evb,
    [](std::shared_ptr<Impl> impl, std::string body,
       std::map<std::string, std::string> headers)
      -> Task<Result<HttpResponse, std::string>> {
      co_return (
        co_await async_try(
          std::invoke([&]() -> Task<HttpResponse> {
            auto sr = co_await impl->pool->getSessionWithReservation();
            TENZIR_ASSERT_ALWAYS(sr.session);
            auto* source
              = make_request_source(impl->url, proxygen::HTTPMethod::POST,
                                    std::move(headers), std::move(body));
            auto resp = proxygen::coro::HTTPClient::Response{};
            co_await proxygen::coro::HTTPClient::request(
              sr.session, std::move(sr.reservation), source,
              proxygen::coro::HTTPClient::makeDefaultReader(resp),
              impl->config.request_timeout);
            co_return to_http_response(resp);
          })))
        .map_err([](folly::exception_wrapper const& e) {
          return e.what().toStdString();
        });
    }(impl_, std::move(body), std::move(headers)));
}

auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout)
  -> Task<Result<HttpResponse, std::string>> {
  discover_and_set_ca_paths();
  co_return co_await co_withExecutor(
    evb,
    [](folly::EventBase* evb, std::string url, std::string body,
       std::map<std::string, std::string> headers,
       std::chrono::milliseconds timeout)
      -> Task<Result<HttpResponse, std::string>> {
      co_return (
        co_await async_try([&]() -> Task<HttpResponse> {
          auto parsed = proxygen::URL{url};
          if (not parsed.isValid() or not parsed.hasHost()) {
            throw std::runtime_error(fmt::format("invalid url: {}", url));
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
          auto* source
            = make_request_source(parsed, proxygen::HTTPMethod::POST,
                                  std::move(headers), std::move(body));
          auto resp = proxygen::coro::HTTPClient::Response{};
          co_await proxygen::coro::HTTPClient::request(
            session, std::move(*reservation), source,
            proxygen::coro::HTTPClient::makeDefaultReader(resp), timeout);
          co_return to_http_response(resp);
        }()))
        .map_err([](folly::exception_wrapper const& e) {
          return e.what().toStdString();
        });
    }(evb, std::move(url), std::move(body), std::move(headers), timeout));
}

} // namespace tenzir
