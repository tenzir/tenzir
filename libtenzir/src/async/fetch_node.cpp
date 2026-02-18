//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/fetch_node.hpp"

#include "tenzir/async/mail.hpp"
#include "tenzir/async/mutex.hpp"
#include "tenzir/connect_to_node.hpp"
#include "tenzir/connector.hpp"

#include <caf/actor_registry.hpp>

namespace tenzir {

auto fetch_node(caf::actor_system& sys, diagnostic_handler& dh)
  -> Task<failure_or<node_actor>> {
  // Fast path: check local registry for existing node.
  if (auto node = sys.registry().template get<node_actor>("tenzir.node")) {
    co_return node;
  }
  static auto mut = RawMutex{};
  const auto lock = co_await mut.lock();
  if (auto node = sys.registry().template get<node_actor>("tenzir.node")) {
    co_return node;
  }
  // Get configuration.
  const auto& opts = content(sys.config());
  const auto node_endpoint = detail::get_node_endpoint(opts);
  if (not node_endpoint) {
    diagnostic::error("failed to get node endpoint: {}", node_endpoint.error())
      .emit(dh);
    co_return failure::promise();
  }
  const auto timeout = detail::node_connection_timeout(opts);
  const auto retry_delay = detail::get_retry_delay(opts);
  const auto deadline = detail::get_deadline(timeout);
  // Spawn connector and request connection.
  auto connector_actor = sys.spawn(connector, retry_delay, deadline, false);
  auto request = connect_request{
    .port = node_endpoint->port->number(),
    .host = node_endpoint->host,
  };
  auto result = co_await async_mail(atom::connect_v, std::move(request))
                  .request(connector_actor);
  caf::anon_send_exit(connector_actor, caf::exit_reason::user_shutdown);
  if (not result) {
    diagnostic::error("failed to connect to node: {}", result.error()).emit(dh);
    co_return failure::promise();
  }
  // Put the actor into the local registry for process-wide fast pathing.
  sys.registry().put("tenzir.node", *result);
  co_return std::move(*result);
}

} // namespace tenzir
