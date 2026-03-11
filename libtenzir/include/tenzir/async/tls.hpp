//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"

#include <memory>
#include <string>

namespace folly {
class SSLContext;

namespace coro {
class Transport;
} // namespace coro
} // namespace folly

namespace tenzir {

/// Upgrade an existing connected transport to TLS as a client.
auto upgrade_transport_to_tls_client(
  folly::coro::Transport transport,
  std::shared_ptr<folly::SSLContext> ssl_context, std::string hostname)
  -> Task<folly::coro::Transport>;

/// Upgrade an existing accepted transport to TLS as a server.
auto upgrade_transport_to_tls_server(
  folly::coro::Transport transport,
  std::shared_ptr<folly::SSLContext> ssl_context)
  -> Task<folly::coro::Transport>;

/// Backward-compatible alias for client-side upgrade.
auto upgrade_transport_to_tls(folly::coro::Transport transport,
                              std::shared_ptr<folly::SSLContext> ssl_context,
                              std::string hostname)
  -> Task<folly::coro::Transport>;

} // namespace tenzir
