//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/result.hpp>

#include <folly/Executor.h>

#include <chrono>
#include <map>
#include <string>

namespace folly {
class EventBase;
class IOExecutor;
} // namespace folly

namespace tenzir {

struct HttpResponse {
  uint16_t status_code = 0;
  std::map<std::string, std::string> headers;
  std::string body;
};

struct HttpPoolConfig {
  bool tls = true;
  std::chrono::milliseconds request_timeout = std::chrono::seconds{90};
};

/// Coroutine-friendly HTTP connection pool backed by proxygen.
///
/// Requests are automatically dispatched on the pool's event base thread.
/// Use `HttpPool::make()` to construct; this ensures proper destruction on
/// the event base thread.
class HttpPool {
public:
  /// Creates a pool bound to the given IO executor and its current event base.
  static auto make(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                   std::string url, HttpPoolConfig config = {})
    -> Box<HttpPool>;

  ~HttpPool();
  HttpPool(HttpPool const&) = delete;
  auto operator=(HttpPool const&) -> HttpPool& = delete;
  HttpPool(HttpPool&&) noexcept;
  auto operator=(HttpPool&&) noexcept -> HttpPool&;

  /// POST through the session pool.
  auto post(std::string body, std::map<std::string, std::string> headers)
    -> Task<Result<HttpResponse, std::string>>;

private:
  explicit HttpPool(folly::Executor::KeepAlive<folly::IOExecutor> executor,
                    std::string url, HttpPoolConfig config);

  struct Impl;
  std::shared_ptr<Impl> impl_;
};

/// One-shot HTTP POST without a connection pool.
///
/// Automatically dispatches the request on the given event base thread.
auto http_post(folly::EventBase* evb, std::string url, std::string body,
               std::map<std::string, std::string> headers,
               std::chrono::milliseconds timeout = std::chrono::seconds{90})
  -> Task<Result<HttpResponse, std::string>>;

} // namespace tenzir
