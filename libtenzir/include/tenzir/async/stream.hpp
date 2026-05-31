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
#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <folly/ExceptionWrapper.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/io/coro/Transport.h>

#include <chrono>
#include <cstddef>
#include <string>
#include <utility>

namespace tenzir {

auto read_stream_chunk(folly::coro::Transport& transport, size_t buffer_size,
                       std::chrono::milliseconds timeout)
  -> Task<Option<chunk_ptr>>;

inline constexpr auto should_retry_socket
  = [](folly::exception_wrapper const& ew) {
      return ew.is_compatible_with<folly::AsyncSocketException>();
    };

inline auto describe_socket_error(folly::AsyncSocketException const& ex)
  -> std::string {
  if (auto err = ex.getErrno(); err > 0) {
    return folly::errnoStr(err);
  }
  return ex.what();
}

inline auto as_byte_range(chunk_ptr const& chunk) -> folly::ByteRange {
  return {
    reinterpret_cast<unsigned char const*>(chunk->data()),
    chunk->size(),
  };
}

namespace detail {

inline auto run_on_transport_event_base(folly::coro::Transport& transport,
                                        auto action) -> void {
  auto* evb = transport.getEventBase();
  TENZIR_ASSERT(evb);
  evb->runInEventBaseThread(std::move(action));
}

} // namespace detail

inline auto close_transport(folly::coro::Transport transport) -> void {
  detail::run_on_transport_event_base(
    transport, [transport = std::move(transport)]() mutable {
      transport.close();
    });
}

inline auto close_transport(Box<folly::coro::Transport> transport) -> void {
  detail::run_on_transport_event_base(
    *transport, [transport = std::move(transport)]() mutable {
      transport->close();
    });
}

inline auto close_transport(Arc<folly::coro::Transport> transport) -> void {
  detail::run_on_transport_event_base(
    *transport, [transport = std::move(transport)]() mutable {
      transport->close();
    });
}

} // namespace tenzir
