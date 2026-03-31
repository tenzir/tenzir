//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/task.hpp"

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/Error.h>
#include <folly/coro/Sleep.h>

#include <chrono>

namespace tenzir {

using namespace std::chrono;

auto sleep_for(steady_clock::duration d) -> Task<void> {
  return folly::coro::sleep(duration_cast<folly::HighResDuration>(d));
}

auto sleep_until(steady_clock::time_point t) -> Task<void> {
  auto now = steady_clock::now();
  // The check is needed because `-` can overflow and yield unexpected results.
  auto diff = t < now ? steady_clock::duration::zero() : t - now;
  return sleep_for(diff);
}

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

auto assert_cancelled() -> Task<void> {
  auto& token = co_await folly::coro::co_current_cancellation_token;
  TENZIR_ASSERT(token.isCancellationRequested());
  co_yield folly::coro::co_stopped_may_throw;
  TENZIR_UNREACHABLE();
}

} // namespace tenzir
