//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/option.hpp"

#include <folly/coro/Sleep.h>
#include <folly/fibers/Semaphore.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/ServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <chrono>
#include <functional>
#include <string>
#include <utility>

namespace tenzir::detail {

template <class OnAccept>
auto run_tcp_accept_loop(
  folly::coro::ServerSocket& server, folly::EventBase* evb,
  folly::fibers::Semaphore& connection_slots, location const& endpoint_source,
  std::string endpoint_description, diagnostic_handler& dh,
  std::chrono::milliseconds retry_delay, OnAccept&& on_accept) -> Task<void> {
  TENZIR_ASSERT(evb);
  while (true) {
    auto accept_error = Option<std::string>{};
    co_await connection_slots.co_wait();
    auto release_connection_slot_guard
      = scope_guard{[&connection_slots]() noexcept {
          connection_slots.signal();
        }};
    try {
      auto transport
        = co_await folly::coro::co_withExecutor(evb, server.accept());
      std::invoke(on_accept, std::move(transport));
      release_connection_slot_guard.disable();
    } catch (folly::AsyncSocketException const& ex) {
      // Accept failures are per-connection network errors; keep the listener
      // alive and continue accepting new clients.
      accept_error = ex.what();
    }
    if (accept_error) {
      diagnostic::warning("failed to accept incoming connection")
        .primary(endpoint_source)
        .note("endpoint: {}", endpoint_description)
        .note("reason: {}", *accept_error)
        .emit(dh);
      co_await folly::coro::sleep(retry_delay);
    }
  }
}

} // namespace tenzir::detail
