//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/tcp.hpp"

#include "tenzir/as_bytes.hpp"
#include "tenzir/async/tls.hpp"
#include "tenzir/detail/assert.hpp"

#include <folly/SocketAddress.h>
#include <folly/io/IOBufQueue.h>
#include <folly/io/coro/Transport.h>
#include <folly/io/coro/TransportCallbacks.h>

namespace tenzir {

auto read_tcp_chunk(folly::coro::Transport& transport, size_t buffer_size,
                    std::chrono::milliseconds timeout)
  -> Task<tcp_read_result> {
  auto* evb = transport.getEventBase();
  TENZIR_ASSERT(evb);
  auto* async_transport = transport.getTransport();
  TENZIR_ASSERT(async_transport);
  auto buffer = folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
  auto callback = folly::coro::ReadCallback{
    evb->timer(), *async_transport, &buffer, 1, buffer_size, timeout,
  };
  async_transport->setReadCB(&callback);
  co_await callback.wait();
  async_transport->setReadCB(nullptr);
  if (callback.error()) {
    callback.error().throw_exception();
  }
  auto length = buffer.chainLength();
  if (length == 0) {
    co_return None{};
  }
  auto iobuf = buffer.move();
  auto range = iobuf->coalesce();
  co_return chunk::make(as_bytes(range.data(), range.size()),
                        [buf = std::move(iobuf)]() noexcept {
                          static_cast<void>(buf);
                        });
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
