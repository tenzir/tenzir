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

#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>

namespace tenzir {

TEST("session tracks active transfers") {
  auto session = CurlSession::make(folly::getGlobalIOExecutor());
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
    auto session = CurlSession::make(folly::getGlobalIOExecutor());
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
    auto session = CurlSession::make(folly::getGlobalIOExecutor());
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
    auto session = CurlSession::make(folly::getGlobalIOExecutor());
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

} // namespace tenzir
