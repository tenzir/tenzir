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

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/ScopedEventBaseThread.h>
#define nsel_CONFIG_SELECT_EXPECTED 1
#include <proxygen/lib/http/coro/server/HTTPCoroAcceptor.h>
#include <proxygen/lib/services/AcceptorConfiguration.h>

#include <limits>
#include <memory>
#include <utility>

namespace tenzir::detail {

inline constexpr auto max_http_server_connections
  = uint64_t{std::numeric_limits<uint32_t>::max()};

class HttpServerRunner final {
public:
  HttpServerRunner(
    folly::SocketAddress address,
    std::shared_ptr<const proxygen::AcceptorConfiguration> acceptor_config,
    std::shared_ptr<proxygen::coro::HTTPHandler> handler)
    : address_{std::move(address)},
      acceptor_config_{std::move(acceptor_config)},
      handler_{std::move(handler)} {
  }

  HttpServerRunner(HttpServerRunner const&) = delete;
  auto operator=(HttpServerRunner const&) -> HttpServerRunner& = delete;
  HttpServerRunner(HttpServerRunner&&) = delete;
  auto operator=(HttpServerRunner&&) -> HttpServerRunner& = delete;

  ~HttpServerRunner() {
    stop();
  }

  auto start() -> Option<std::string> {
    evb_thread_ = Box<folly::ScopedEventBaseThread>{std::in_place};
    auto error = Option<std::string>{};
    (*evb_thread_)->getEventBase()->runInEventBaseThreadAndWait([this, &error] {
      try {
        socket_ = folly::AsyncServerSocket::UniquePtr(
          new folly::AsyncServerSocket{(*evb_thread_)->getEventBase()});
        socket_->bind(address_);
        socket_->listen(acceptor_config_->acceptBacklog);
        socket_->startAccepting();
        acceptor_ = std::make_unique<proxygen::coro::HTTPCoroAcceptor>(
          acceptor_config_, handler_);
        acceptor_->init(socket_.get(), (*evb_thread_)->getEventBase());
      } catch (std::exception const& ex) {
        error = std::string{ex.what()};
      }
    });
    if (error) {
      stop();
    }
    return error;
  }

  auto request_stop_accepting() -> void {
    if (not evb_thread_) {
      return;
    }
    (*evb_thread_)->getEventBase()->runInEventBaseThreadAndWait([this] {
      if (socket_) {
        socket_->stopAccepting();
      }
    });
  }

  auto stop() -> void {
    if (not evb_thread_) {
      return;
    }
    (*evb_thread_)->getEventBase()->runInEventBaseThreadAndWait([this] {
      if (acceptor_) {
        acceptor_->forceStop();
      }
      if (socket_) {
        socket_->stopAccepting();
      }
    });
    std::ignore = acceptor_.release();
    std::ignore = socket_.release();
    evb_thread_ = None{};
  }

private:
  folly::SocketAddress address_;
  std::shared_ptr<const proxygen::AcceptorConfiguration> acceptor_config_;
  std::shared_ptr<proxygen::coro::HTTPHandler> handler_;
  folly::AsyncServerSocket::UniquePtr socket_;
  std::unique_ptr<proxygen::coro::HTTPCoroAcceptor> acceptor_;
  Option<Box<folly::ScopedEventBaseThread>> evb_thread_;
};

} // namespace tenzir::detail
