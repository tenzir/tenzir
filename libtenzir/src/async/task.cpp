//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/task.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>

namespace tenzir {

auto wait_forever() -> Task<void> {
  // We want to stop this when cancellation occurs, but `Baton` is not
  // cancelable by default.
  auto baton = folly::coro::Baton{};
  auto& token = co_await folly::coro::co_current_cancellation_token;
  auto callback = folly::CancellationCallback{token, [&]() noexcept {
                                                baton.post();
                                              }};
  co_await baton;
  if (token.isCancellationRequested()) {
    co_yield folly::coro::co_stopped_may_throw;
  }
}

} // namespace tenzir
