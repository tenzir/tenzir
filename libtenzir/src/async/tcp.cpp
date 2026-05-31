//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/tcp.hpp"

#include "tenzir/async/tls.hpp"

#include <folly/SocketAddress.h>
#include <folly/io/coro/Transport.h>

namespace tenzir {

auto read_tcp_chunk(folly::coro::Transport& transport, size_t buffer_size,
                    std::chrono::milliseconds timeout)
  -> Task<tcp_read_result> {
  co_return co_await read_stream_chunk(transport, buffer_size, timeout);
}

auto connect_tcp_client(folly::EventBase* evb,
                        folly::SocketAddress const& address,
                        std::chrono::milliseconds timeout,
                        std::shared_ptr<folly::SSLContext> ssl_context,
                        std::string hostname) -> Task<folly::coro::Transport> {
  auto transport = co_await folly::coro::co_withExecutor(
    evb, folly::coro::Transport::newConnectedSocket(evb, address, timeout));
  if (not ssl_context) {
    co_return transport;
  }
  co_return co_await upgrade_transport_to_tls_client(
    std::move(transport), std::move(ssl_context), std::move(hostname));
}

} // namespace tenzir
