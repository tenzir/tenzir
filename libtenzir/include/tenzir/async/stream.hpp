//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/option.hpp"

#include <folly/ExceptionWrapper.h>
#include <folly/io/async/AsyncSocketException.h>

#include <chrono>
#include <cstddef>
#include <string>

namespace folly::coro {
class Transport;
} // namespace folly::coro

namespace tenzir {

auto read_stream_chunk(folly::coro::Transport& transport, size_t buffer_size,
                       std::chrono::milliseconds timeout)
  -> Task<Option<chunk_ptr>>;

auto close_stream_transport(folly::coro::Transport transport) -> void;

auto close_stream_transport(Box<folly::coro::Transport> transport) -> void;

auto close_stream_transport(Arc<folly::coro::Transport> transport) -> void;

auto describe_socket_error(folly::AsyncSocketException const& ex)
  -> std::string;

inline constexpr auto should_retry_socket
  = [](folly::exception_wrapper const& ew) {
      return ew.is_compatible_with<folly::AsyncSocketException>();
    };

} // namespace tenzir
