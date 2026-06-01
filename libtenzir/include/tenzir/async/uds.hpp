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
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"

#include <folly/SocketAddress.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/coro/Transport.h>

#include <cstdint>
#include <string>

namespace tenzir {

class UdsServerSocket {
public:
  UdsServerSocket(Arc<folly::AsyncServerSocket> socket,
                  folly::SocketAddress const& address,
                  uint32_t listen_queue_depth);

  auto accept() -> Task<Box<folly::coro::Transport>>;

  auto close() noexcept -> void;

private:
  Arc<folly::AsyncServerSocket> socket_;
};

auto make_uds_socket_address(std::string const& path, location source,
                             diagnostic_handler& dh)
  -> failure_or<folly::SocketAddress>;

auto prepare_uds_listen_path(std::string const& path, location source,
                             diagnostic_handler& dh) -> failure_or<void>;

} // namespace tenzir
