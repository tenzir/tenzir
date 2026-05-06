//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"

#include <folly/Unit.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/FutureUtil.h>
#include <folly/futures/Future.h>

#include <concepts>

namespace tenzir {

/// Wraps a `SemiFuture` into a `Task` that requests interruption of the future
/// when the surrounding task is cancelled.
///
/// Workaround for a bug in folly's `toTaskInterruptOnCancel(SemiFuture<V>)`
/// overload: for non-`Unit` `V` it falls off the end without `co_return`,
/// leaving the resulting `Task`'s `Try` uninitialized so that awaiting it
/// throws `folly::UsingUninitializedTry`. We sidestep the broken overload by
/// converting the `SemiFuture` to a `Future` here and using the (correct)
/// `Future` overload directly.
template <class T>
auto to_task_interrupt_on_cancel(folly::SemiFuture<T> f)
  -> Task<folly::drop_unit_t<T>> {
  auto ex = co_await folly::coro::co_current_executor;
  if constexpr (std::same_as<T, folly::Unit>) {
    co_await folly::coro::toTaskInterruptOnCancel(
      std::move(f).via(std::move(ex)));
  } else {
    co_return co_await folly::coro::toTaskInterruptOnCancel(
      std::move(f).via(std::move(ex)));
  }
}

} // namespace tenzir
