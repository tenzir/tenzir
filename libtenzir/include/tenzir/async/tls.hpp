//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\\ \\  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/task.hpp"
#include "tenzir/box.hpp"

#include <memory>
#include <string>

namespace folly {
class EventBase;
class SSLContext;

namespace coro {
class Transport;
} // namespace coro
} // namespace folly

namespace tenzir {

/// Upgrade an existing connected transport to TLS in-place.
auto upgrade_transport_to_tls(Box<folly::coro::Transport>& transport,
                              folly::EventBase* evb,
                              std::shared_ptr<folly::SSLContext> ssl_context,
                              std::string hostname) -> Task<void>;

} // namespace tenzir
