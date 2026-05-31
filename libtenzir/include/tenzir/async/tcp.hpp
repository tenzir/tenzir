//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/stream.hpp"
#include "tenzir/async/task.hpp"

#include <chrono>
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

using tcp_read_result = stream_read_result;

auto read_tcp_chunk(folly::coro::Transport& transport, size_t buffer_size,
                    std::chrono::milliseconds timeout) -> Task<tcp_read_result>;

auto connect_tcp_client(folly::EventBase* evb,
                        folly::SocketAddress const& address,
                        std::chrono::milliseconds timeout,
                        std::shared_ptr<folly::SSLContext> ssl_context = {},
                        std::string hostname = {})
  -> Task<folly::coro::Transport>;

} // namespace tenzir
