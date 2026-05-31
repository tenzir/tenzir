//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/option.hpp"

#include <chrono>
#include <cstddef>

namespace folly::coro {
class Transport;
} // namespace folly::coro

namespace tenzir {

using stream_read_result = Option<chunk_ptr>;

auto read_stream_chunk(folly::coro::Transport& transport, size_t buffer_size,
                       std::chrono::milliseconds timeout)
  -> Task<stream_read_result>;

} // namespace tenzir
