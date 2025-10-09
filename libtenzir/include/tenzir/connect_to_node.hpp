//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/connect_request.hpp"
#include "tenzir/connector.hpp"
#include "tenzir/endpoint.hpp"

#include <caf/event_based_actor.hpp>

namespace tenzir {

namespace detail {

/// Retrieves the node connection timeout as specified under the option
/// `tenzir.connection-timeout` from the given settings.
auto node_connection_timeout(const caf::settings& options) -> caf::timespan;

auto get_node_endpoint(const caf::settings& opts) -> caf::expected<endpoint>;

auto get_retry_delay(const caf::settings& settings)
  -> std::optional<caf::timespan>;

auto get_deadline(caf::timespan timeout)
  -> std::optional<std::chrono::steady_clock::time_point>;

[[nodiscard]] auto
check_version(const record& remote_version, const record& cfg) -> bool;

} // namespace detail

/// Connects to a remote Tenzir server.
auto connect_to_node(caf::scoped_actor& self, endpoint endpoint,
                     caf::timespan timeout,
                     std::optional<caf::timespan> retry_delay = std::nullopt,
                     bool internal_connection = false)
  -> caf::expected<node_actor>;

auto connect_to_node(caf::scoped_actor& self, bool internal_connection = false)
  -> caf::expected<node_actor>;

template <class... Sigs>
void connect_to_node(caf::typed_event_based_actor<Sigs...>* self,
                     std::function<void(caf::expected<node_actor>)> callback,
                     bool internal_connection = false) {
  // Fetch values from config.
  const auto& opts = content(self->system().config());
  auto node_endpoint = detail::get_node_endpoint(opts);
  if (! node_endpoint) {
    return callback(std::move(node_endpoint.error()));
  }
  const auto timeout = detail::node_connection_timeout(opts);
  auto connector
    = self->spawn(tenzir::connector, detail::get_retry_delay(opts),
                  detail::get_deadline(timeout), internal_connection);
  self
    ->mail(atom::connect_v,
           connect_request{node_endpoint->port->number(), node_endpoint->host})
    .request(connector, caf::infinite)
    .then(
      [=](node_actor& node) {
        // We must keep the connector alive until after this request is completed.
        (void)connector;
        self->mail(atom::get_v, atom::version_v)
          .request(node, timeout)
          .then(
            [self, callback,
             node = std::move(node)](record& remote_version) mutable {
              // TODO: Refactor this (also in .cpp).
              (void)detail::check_version(
                remote_version,
                check(to<record>(content(self->system().config()))));
              callback(std::move(node));
            },
            [=](caf::error& error) {
              callback(
                caf::make_error(ec::version_error,
                                fmt::format("failed to receive remote version "
                                            "within specified "
                                            "connection-timeout of {}: {}",
                                            timeout, std::move(error))));
            });
      },
      [=](caf::error& err) {
        callback(std::move(err));
      });
}

} // namespace tenzir
