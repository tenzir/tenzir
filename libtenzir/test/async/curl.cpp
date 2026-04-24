//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/curl.hpp"

#include "tenzir/panic.hpp"
#include "tenzir/test/test.hpp"

#ifdef INFO
#  undef INFO
#endif

#include <folly/coro/BlockingWait.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <chrono>
#include <thread>

namespace tenzir {

TEST("session tracks active transfers") {
  auto io_thread = folly::ScopedEventBaseThread{};
  auto session = CurlSession::make(folly::getKeepAliveToken(io_thread));
  check(not session.busy());
  folly::coro::blockingWait([&]() -> Task<void> {
    auto transfer = session.start_upload();
    check(session.busy());
    transfer.close();
    auto result = co_await transfer.result();
    require(result.is_ok());
    check_eq(result.unwrap(), CurlTransferStatus::finished);
  }());
  check(not session.busy());
}

TEST("session refuses concurrent transfers") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto io_thread = folly::ScopedEventBaseThread{};
    auto session = CurlSession::make(folly::getKeepAliveToken(io_thread));
    auto transfer = session.start_upload();
    auto refused = false;
    try {
      auto other = session.start_download();
      TENZIR_UNUSED(other);
    } catch (panic_exception const&) {
      refused = true;
    }
    check(refused);
    transfer.close();
    auto result = co_await transfer.result();
    require(result.is_ok());
    check_eq(result.unwrap(), CurlTransferStatus::finished);
  }());
}

TEST("empty send completes without starting curl") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto io_thread = folly::ScopedEventBaseThread{};
    auto session = CurlSession::make(folly::getKeepAliveToken(io_thread));
    auto send = session.start_upload();
    send.close();
    auto result = co_await send.result();
    require(result.is_ok());
    check_eq(result.unwrap(), CurlTransferStatus::finished);
    check(not session.busy());
  }());
}

TEST("session reuses completed transfers") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto io_thread = folly::ScopedEventBaseThread{};
    auto session = CurlSession::make(folly::getKeepAliveToken(io_thread));
    auto first = session.start_upload();
    first.close();
    auto first_result = co_await first.result();
    require(first_result.is_ok());
    check_eq(first_result.unwrap(), CurlTransferStatus::finished);
    auto second = session.start_upload();
    check(session.busy());
    second.close();
    auto second_result = co_await second.result();
    require(second_result.is_ok());
    check_eq(second_result.unwrap(), CurlTransferStatus::finished);
    check(not session.busy());
  }());
}

TEST("session remains busy until upload result is observed") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto io_thread = folly::ScopedEventBaseThread{};
    auto session = CurlSession::make(folly::getKeepAliveToken(io_thread));
    auto transfer = session.start_upload();
    transfer.close();
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
    check(session.busy());
    auto refused = false;
    try {
      auto other = session.start_upload();
      TENZIR_UNUSED(other);
    } catch (panic_exception const&) {
      refused = true;
    }
    check(refused);
    auto result = co_await transfer.result();
    require(result.is_ok());
    check_eq(result.unwrap(), CurlTransferStatus::finished);
    check(not session.busy());
  }());
}

} // namespace tenzir
