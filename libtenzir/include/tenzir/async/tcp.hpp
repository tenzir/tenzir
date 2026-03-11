//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/chunk.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

namespace folly {
class EventBase;
class SSLContext;
class SocketAddress;

namespace coro {
class Transport;
} // namespace coro
} // namespace folly

namespace tenzir {

struct tcp_read_result {
  chunk_ptr chunk;
  bool eof = false;
};

auto read_tcp_chunk(Box<folly::coro::Transport>& transport, size_t buffer_size,
                    std::chrono::milliseconds timeout) -> Task<tcp_read_result>;

auto connect_tcp_client(folly::EventBase* evb,
                        folly::SocketAddress const& address,
                        std::chrono::milliseconds timeout,
                        std::shared_ptr<folly::SSLContext> ssl_context = {},
                        std::string hostname = {})
  -> Task<Box<folly::coro::Transport>>;

} // namespace tenzir
