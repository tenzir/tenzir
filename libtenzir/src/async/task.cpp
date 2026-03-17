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
#include <folly/coro/Sleep.h>

#include <chrono>

namespace tenzir {

auto sleep_for(duration d) -> Task<void> {
  return folly::coro::sleep(
    std::chrono::duration_cast<folly::HighResDuration>(d));
}

auto sleep_until(time t) -> Task<void> {
  auto now = time::clock::now();
  // The check is needed because `-` can overflow and yield unexpected results.
  auto diff = t < now ? duration{0} : t - now;
  return sleep_for(diff);
}

auto wait_forever() -> Task<void> {
  // We want to stop this when cancellation occurs, but `Baton` is not
  // cancelable by default.
  auto baton = folly::coro::Baton{};
  auto token = co_await folly::coro::co_current_cancellation_token;
  auto callback = folly::CancellationCallback{token, [&]() noexcept {
                                                baton.post();
                                              }};
  co_await baton;
  if (token.isCancellationRequested()) {
    co_yield folly::coro::co_cancelled;
  }
}

} // namespace tenzir
