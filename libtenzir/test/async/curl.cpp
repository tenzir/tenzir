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
  auto transfer = session.start_perform();
  check(session.busy());
  transfer.cancel();
  check(not session.busy());
}

TEST("session refuses concurrent transfers") {
  auto session = CurlSession::make(folly::getGlobalIOExecutor());
  auto transfer = session.start_perform();
  auto refused = false;
  try {
    auto other = session.start_perform();
    TENZIR_UNUSED(other);
  } catch (panic_exception const&) {
    refused = true;
  }
  transfer.cancel();
  check(refused);
}

TEST("empty send completes without starting curl") {
  folly::coro::blockingWait([&]() -> Task<void> {
    auto session = CurlSession::make(folly::getGlobalIOExecutor());
    auto send = session.start_send();
    send.close();
    auto result = co_await send.wait();
    require(result.is_ok());
    check_eq(result.unwrap().kind, CurlCompletionKind::finished);
    check(not session.busy());
  }());
}

} // namespace tenzir
