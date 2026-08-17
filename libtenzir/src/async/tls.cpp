//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/tls.hpp"

#include "tenzir/detail/assert.hpp"

#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/coro/Transport.h>
#include <folly/io/coro/TransportCallbackBase.h>
#include <openssl/x509v3.h>

#include <array>
#include <span>

namespace tenzir {

namespace {

// Give certificate exchange and verification enough time to complete on a
// healthy peer, but fail fast enough that broken TLS setup feeds back into the
// caller's reconnect logic promptly.
constexpr auto tls_handshake_timeout = std::chrono::seconds{5};

class client_ssl_handshake_callback
  : public folly::coro::TransportCallbackBase,
    public folly::AsyncSSLSocket::HandshakeCB {
public:
  explicit client_ssl_handshake_callback(folly::AsyncSSLSocket& socket,
                                         std::string hostname)
    : TransportCallbackBase(socket),
      socket_(socket),
      hostname_(std::move(hostname)) {
  }

private:
  void cancel() noexcept override {
    socket_.closeNow();
  }

  auto handshakeVer(folly::AsyncSSLSocket*, bool preverify_ok,
                    X509_STORE_CTX* ctx) noexcept -> bool override {
    if (not preverify_ok) {
      return false;
    }
    if (hostname_.empty() or X509_STORE_CTX_get_error_depth(ctx) != 0) {
      return true;
    }
    auto* cert = X509_STORE_CTX_get_current_cert(ctx);
    if (not cert) {
      return false;
    }
    return X509_check_host(cert, hostname_.c_str(), hostname_.size(), 0,
                           nullptr)
           == 1;
  }

  void handshakeSuc(folly::AsyncSSLSocket*) noexcept override {
    post();
  }

  void handshakeErr(folly::AsyncSSLSocket*,
                    folly::AsyncSocketException const& ex) noexcept override {
    storeException(ex);
    post();
  }

  folly::AsyncSSLSocket& socket_;
  std::string hostname_;
};

class server_ssl_handshake_callback
  : public folly::coro::TransportCallbackBase,
    public folly::AsyncSSLSocket::HandshakeCB {
public:
  explicit server_ssl_handshake_callback(folly::AsyncSSLSocket& socket)
    : TransportCallbackBase(socket), socket_(socket) {
  }

private:
  void cancel() noexcept override {
    socket_.closeNow();
  }

  void handshakeSuc(folly::AsyncSSLSocket*) noexcept override {
    post();
  }

  void handshakeErr(folly::AsyncSSLSocket*,
                    folly::AsyncSocketException const& ex) noexcept override {
    storeException(ex);
    post();
  }

  folly::AsyncSSLSocket& socket_;
};

auto get_socket_transport(folly::coro::Transport& transport)
  -> folly::AsyncSocket* {
  auto* raw_transport = transport.getTransport();
  auto* socket = dynamic_cast<folly::AsyncSocket*>(raw_transport);
  if (not socket) {
    throw folly::AsyncSocketException{
      folly::AsyncSocketException::INTERNAL_ERROR,
      "transport is not backed by AsyncSocket",
    };
  }
  return socket;
}

auto close_transport(folly::coro::Transport transport) -> void {
  auto* evb = transport.getEventBase();
  TENZIR_ASSERT(evb);
  evb->runInEventBaseThread([transport = std::move(transport)]() mutable {
    transport.close();
  });
}

auto could_be_tls_client_hello(std::span<std::byte const> bytes) -> bool {
  if (bytes.empty() or bytes[0] != std::byte{0x16}) {
    return false;
  }
  if (bytes.size() >= 2 and bytes[1] != std::byte{0x03}) {
    return false;
  }
  if (bytes.size() >= 3 and bytes[2] > std::byte{0x04}) {
    return false;
  }
  if (bytes.size() >= 6 and bytes[5] != std::byte{0x01}) {
    return false;
  }
  return true;
}

} // namespace

auto probe_tls_client_hello(folly::coro::Transport& transport,
                            std::chrono::milliseconds timeout) -> Task<bool> {
  auto prefix = std::array<std::byte, 6>{};
  auto size = size_t{0};
  auto deadline = std::chrono::steady_clock::now() + timeout;
  auto throw_timed_out = [] {
    throw folly::AsyncSocketException{
      folly::AsyncSocketException::TIMED_OUT,
      "TLS auto-detect probe timed out",
    };
  };
  auto read_some = [&](size_t min_size) -> Task<void> {
    while (size < min_size) {
      auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        throw_timed_out();
      }
      auto remaining
        = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      if (remaining <= std::chrono::milliseconds{0}) {
        throw_timed_out();
      }
      auto* data = reinterpret_cast<unsigned char*>(prefix.data() + size);
      auto count
        = co_await transport.read(folly::MutableByteRange{data, 1}, remaining);
      if (count == 0) {
        co_return;
      }
      size += count;
      if (not could_be_tls_client_hello(std::span{prefix.data(), size})) {
        co_return;
      }
    }
  };
  auto timed_out = false;
  try {
    co_await read_some(1);
    if (size == 0) {
      co_return false;
    }
    if (prefix[0] == std::byte{0x16}) {
      co_await read_some(prefix.size());
    }
  } catch (folly::AsyncSocketException const& ex) {
    if (ex.getType() != folly::AsyncSocketException::TIMED_OUT) {
      throw;
    }
    timed_out = true;
  }
  auto bytes = std::span{prefix.data(), size};
  if (not bytes.empty()) {
    auto* socket = get_socket_transport(transport);
    socket->setPreReceivedData(
      folly::IOBuf::copyBuffer(bytes.data(), bytes.size()));
  }
  co_return (bytes.size() >= prefix.size() and could_be_tls_client_hello(bytes))
    or (timed_out and could_be_tls_client_hello(bytes));
}

auto upgrade_transport_to_tls_client(
  folly::coro::Transport transport,
  std::shared_ptr<folly::SSLContext> ssl_context, std::string hostname)
  -> Task<folly::coro::Transport> {
  auto* evb = transport.getEventBase();
  TENZIR_ASSERT(evb);
  auto [upgraded_transport, callback] = co_await folly::coro::co_withExecutor(
    evb,
    folly::coro::co_invoke(
      [transport = std::move(transport), evb, ctx = std::move(ssl_context),
       hostname = std::move(hostname)]() mutable
        -> Task<std::pair<folly::coro::Transport,
                          std::shared_ptr<client_ssl_handshake_callback>>> {
        auto* old_socket = get_socket_transport(transport);
        auto ssl_socket = folly::AsyncSSLSocket::UniquePtr{
          new folly::AsyncSSLSocket{
            std::move(ctx),
            old_socket,
            /*server=*/false,
            /*deferSecurityNegotiation=*/true,
          },
        };
        auto* ssl_ptr = ssl_socket.get();
        if (not hostname.empty()) {
          ssl_ptr->setServerName(hostname);
        }
        transport = folly::coro::Transport{
          evb,
          folly::AsyncTransport::UniquePtr{ssl_socket.release()},
        };
        auto callback = std::make_shared<client_ssl_handshake_callback>(
          *ssl_ptr, std::move(hostname));
        ssl_ptr->sslConn(callback.get(), tls_handshake_timeout);
        co_return std::pair{std::move(transport), std::move(callback)};
      }));
  try {
    co_await callback->wait();
  } catch (...) {
    close_transport(std::move(upgraded_transport));
    throw;
  }
  if (callback->error()) {
    close_transport(std::move(upgraded_transport));
    callback->error().throw_exception();
  }
  co_return std::move(upgraded_transport);
}

auto upgrade_transport_to_tls_server(
  folly::coro::Transport transport,
  std::shared_ptr<folly::SSLContext> ssl_context)
  -> Task<folly::coro::Transport> {
  auto* evb = transport.getEventBase();
  TENZIR_ASSERT(evb);
  auto [upgraded_transport, callback] = co_await folly::coro::co_withExecutor(
    evb,
    folly::coro::co_invoke(
      [transport = std::move(transport), evb,
       ctx = std::move(ssl_context)]() mutable
        -> Task<std::pair<folly::coro::Transport,
                          std::shared_ptr<server_ssl_handshake_callback>>> {
        auto* old_socket = get_socket_transport(transport);
        auto ssl_socket = folly::AsyncSSLSocket::UniquePtr{
          new folly::AsyncSSLSocket{
            std::move(ctx),
            old_socket,
            /*server=*/true,
            /*deferSecurityNegotiation=*/true,
          },
        };
        auto* ssl_ptr = ssl_socket.get();
        transport = folly::coro::Transport{
          evb,
          folly::AsyncTransport::UniquePtr{ssl_socket.release()},
        };
        auto callback
          = std::make_shared<server_ssl_handshake_callback>(*ssl_ptr);
        ssl_ptr->sslAccept(callback.get(), tls_handshake_timeout);
        co_return std::pair{std::move(transport), std::move(callback)};
      }));
  try {
    co_await callback->wait();
  } catch (...) {
    close_transport(std::move(upgraded_transport));
    throw;
  }
  if (callback->error()) {
    close_transport(std::move(upgraded_transport));
    callback->error().throw_exception();
  }
  co_return std::move(upgraded_transport);
}

auto upgrade_transport_to_tls(folly::coro::Transport transport,
                              std::shared_ptr<folly::SSLContext> ssl_context,
                              std::string hostname)
  -> Task<folly::coro::Transport> {
  co_return co_await upgrade_transport_to_tls_client(
    std::move(transport), std::move(ssl_context), std::move(hostname));
}

} // namespace tenzir
