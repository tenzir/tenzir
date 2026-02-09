//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async_tls.hpp"

#include "tenzir/detail/assert.hpp"

#include <folly/coro/Invoke.h>
#include <folly/coro/Task.h>
#include <folly/coro/Transport.h>
#include <folly/coro/TransportCallbackBase.h>
#include <folly/io/async/AsyncSSLSocket.h>
#include <openssl/x509v3.h>

namespace tenzir {

namespace {

/// Coroutine-friendly callback for SSL/TLS handshake completion.
class ssl_handshake_callback : public folly::coro::TransportCallbackBase,
                               public folly::AsyncSSLSocket::HandshakeCB {
public:
  explicit ssl_handshake_callback(folly::AsyncSSLSocket& socket,
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
    // Only verify hostname on the leaf certificate (depth 0).
    if (X509_STORE_CTX_get_error_depth(ctx) != 0) {
      return true;
    }
    // Verify that the certificate's CN or SAN matches the target hostname.
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

} // namespace

auto upgrade_transport_to_tls(Box<folly::coro::Transport>& transport,
                              folly::EventBase* evb,
                              std::shared_ptr<folly::SSLContext> ssl_context,
                              std::string hostname) -> Task<void> {
  co_await folly::coro::co_withExecutor(
    evb,
    folly::coro::co_invoke([&transport, evb, ctx = std::move(ssl_context),
                            hostname = std::move(hostname)]() mutable
                             -> Task<void> {
      // Get the underlying socket and detach its fd.
      auto* raw_transport = transport->getTransport();
      auto* socket = dynamic_cast<folly::AsyncSocket*>(raw_transport);
      TENZIR_ASSERT(socket);
      auto fd = socket->detachNetworkSocket();
      // Create an SSL socket wrapping the existing fd.
      auto ssl_socket = folly::AsyncSSLSocket::newSocket(
        std::move(ctx), evb, fd,
        /*server=*/false,
        /*deferSecurityNegotiation=*/true);
      auto* ssl_ptr = ssl_socket.get();
      // Replace the transport with one wrapping the SSL socket.
      transport = Box<folly::coro::Transport>{
        std::in_place, evb,
        folly::AsyncTransport::UniquePtr{ssl_socket.release()}};
      // Perform the TLS handshake.
      auto cb = ssl_handshake_callback{*ssl_ptr, std::move(hostname)};
      ssl_ptr->sslConn(&cb);
      co_await cb.wait();
      // Re-throw handshake errors captured in the callback.
      if (cb.error()) {
        cb.error().throw_exception();
      }
      co_return;
    }));
}

} // namespace tenzir
