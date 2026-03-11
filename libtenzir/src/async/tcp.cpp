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

auto read_tcp_chunk(Box<folly::coro::Transport>& transport, size_t buffer_size,
                    std::chrono::milliseconds timeout)
  -> Task<tcp_read_result> {
  auto* evb = transport->getEventBase();
  TENZIR_ASSERT(evb);
  auto* async_transport = transport->getTransport();
  TENZIR_ASSERT(async_transport);
  auto buffer = folly::IOBufQueue{folly::IOBufQueue::cacheChainLength()};
  auto callback = folly::coro::ReadCallback{
    evb->timer(), *async_transport, &buffer, 1, buffer_size, timeout,
  };
  async_transport->setReadCB(&callback);
  auto wait_result = co_await folly::coro::co_awaitTry(callback.wait());
  async_transport->setReadCB(nullptr);
  if (wait_result.hasException()) {
    wait_result.exception().throw_exception();
  }
  if (callback.error()) {
    callback.error().throw_exception();
  }
  auto length = buffer.chainLength();
  if (length == 0) {
    co_return tcp_read_result{.eof = callback.eof};
  }
  auto iobuf = buffer.move();
  auto range = iobuf->coalesce();
  co_return tcp_read_result{
    .chunk = chunk::make(as_bytes(range.data(), range.size()),
                         [buf = std::move(iobuf)]() noexcept {
                           static_cast<void>(buf);
                         }),
    .eof = callback.eof,
  };
}

auto connect_tcp_client(folly::EventBase* evb,
                        folly::SocketAddress const& address,
                        std::chrono::milliseconds timeout,
                        std::shared_ptr<folly::SSLContext> ssl_context,
                        std::string hostname)
  -> Task<Box<folly::coro::Transport>> {
  auto transport = co_await folly::coro::co_withExecutor(
    evb, folly::coro::Transport::newConnectedSocket(evb, address, timeout));
  auto boxed = Box<folly::coro::Transport>{std::move(transport)};
  if (not ssl_context) {
    co_return boxed;
  }
  co_await upgrade_transport_to_tls_client(boxed, std::move(ssl_context),
                                           std::move(hostname));
  co_return boxed;
}

} // namespace tenzir
