//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/async.hpp"
#include "tenzir/async/stream.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pipeline_metrics.hpp"

#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>

namespace tenzir::detail {

constexpr auto stream_connect_timeout = std::chrono::seconds{5};
constexpr auto stream_connect_initial_backoff = std::chrono::milliseconds{100};
constexpr auto stream_connect_max_backoff = std::chrono::seconds{5};
constexpr auto stream_connect_retry_jitter = 0.0;
constexpr auto stream_default_connect_max_retry_count
  = std::numeric_limits<uint32_t>::max();
constexpr auto stream_accept_retry_delay = std::chrono::milliseconds{100};

inline auto stream_sub_key_for(uint64_t conn_id) -> data {
  TENZIR_ASSERT(conn_id
                <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
  return data{int64_t{static_cast<int64_t>(conn_id)}};
}

inline auto stream_close_subpipeline(uint64_t conn_id, OpCtx& ctx)
  -> Task<void> {
  auto key = stream_sub_key_for(conn_id);
  if (auto sub = ctx.get_sub(make_view(key))) {
    auto& pipeline = as<SubHandle<chunk_ptr>>(*sub);
    co_await pipeline.close();
  }
  co_return;
}

template <class Payload, class ConnectionClosed, class MessageQueue,
          class OnRead, class OnClose>
auto stream_read_loop(uint64_t conn_id, Arc<folly::coro::Transport> connection,
                      Arc<MessageQueue> message_queue, size_t buffer_size,
                      MetricsCounter bytes_counter, OnRead on_read,
                      OnClose on_close) -> Task<void> {
  auto read_error = Option<std::string>{};
  while (true) {
    try {
      auto read_result = co_await read_stream_chunk(
        *connection, buffer_size, std::chrono::milliseconds{0});
      if (not read_result) {
        break;
      }
      bytes_counter.add((*read_result)->size());
      on_read((*read_result)->size());
      co_await message_queue->enqueue(
        Payload{conn_id, std::move(*read_result)});
    } catch (folly::AsyncSocketException const& e) {
      read_error = e.what();
      break;
    }
  }
  on_close();
  co_await message_queue->enqueue(
    ConnectionClosed{conn_id, std::move(read_error)});
}

template <class Payload, class ConnectionClosed, class MessageQueue,
          class OnRead>
auto stream_read_loop(uint64_t conn_id, Arc<folly::coro::Transport> connection,
                      Arc<MessageQueue> message_queue, size_t buffer_size,
                      MetricsCounter bytes_counter, OnRead on_read)
  -> Task<void> {
  co_await stream_read_loop<Payload, ConnectionClosed>(
    conn_id, std::move(connection), std::move(message_queue), buffer_size,
    std::move(bytes_counter), std::move(on_read), [] {});
}

} // namespace tenzir::detail
