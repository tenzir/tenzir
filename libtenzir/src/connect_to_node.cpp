//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/connect_to_node.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/concept/parseable/tenzir/endpoint.hpp"
#include "tenzir/connect_request.hpp"
#include "tenzir/connector.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/endpoint.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node_control.hpp"
#include "tenzir/version.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

namespace tenzir {

namespace {

void assert_data_completness(const record& remote_version,
                             const record& local_version) {
  if (!local_version.contains("Tenzir"))
    die("no Tenzir key found in a local version");
  if (!remote_version.contains("Tenzir"))
    die("no Tenzir key found in a remote version");
  if (!local_version.contains("plugins"))
    die("no plugins key found in a local version");
  if (!remote_version.contains("plugins"))
    die("no plugins key found in a remote version");
}

} // namespace

namespace details {

auto get_node_endpoint(const caf::settings& opts) -> caf::expected<endpoint> {
  endpoint node_endpoint;
  auto endpoint_str
    = get_or(opts, "tenzir.endpoint", defaults::endpoint.data());
  if (!parsers::endpoint(endpoint_str, node_endpoint))
    return caf::make_error(ec::parse_error, "invalid endpoint",
                           endpoint_str.data());
  // Default to port 5158/tcp if none is set.
  if (!node_endpoint.port)
    node_endpoint.port = port{defaults::endpoint_port, port_type::tcp};
  if (node_endpoint.port->type() == port_type::unknown)
    node_endpoint.port->type(port_type::tcp);
  if (node_endpoint.port->type() != port_type::tcp)
    return caf::make_error(ec::invalid_configuration, "invalid protocol",
                           *node_endpoint.port);
  if (node_endpoint.host.empty())
    node_endpoint.host = defaults::endpoint_host;
  return node_endpoint;
}

auto node_connection_timeout(const caf::settings& options) -> caf::timespan;

auto get_retry_delay(const caf::settings& settings)
  -> std::optional<caf::timespan> {
  auto retry_delay
    = caf::get_or<caf::timespan>(settings, "tenzir.connection-retry-delay",
                                 defaults::node_connection_retry_delay);
  if (retry_delay == caf::timespan::zero())
    return {};
  return retry_delay;
}

auto get_deadline(caf::timespan timeout)
  -> std::optional<std::chrono::steady_clock::time_point> {
  if (caf::is_infinite(timeout))
    return {};
  return {std::chrono::steady_clock::now() + timeout};
}

[[nodiscard]] auto check_version(const record& remote_version) -> bool {
  const auto local_version = retrieve_versions();
  assert_data_completness(remote_version, local_version);
  if (local_version.at("Tenzir") != remote_version.at("Tenzir")) {
    TENZIR_WARN("client version {} does not match node version {}; "
                "this may cause unexpected behavior",
                local_version.at("Tenzir"), remote_version.at("Tenzir"));
    return false;
  }
  TENZIR_DEBUG("client verified that client version matches node "
               "version {}",
               local_version.at("Tenzir"));
  if (local_version.at("plugins") != remote_version.at("plugins")) {
    TENZIR_WARN("client plugins {} do not match node plugins {}; "
                "this may cause unexpected behavior",
                local_version.at("plugins"), remote_version.at("plugins"));
    return false;
  }
  TENZIR_DEBUG("client verified that local plugins match node plugins {}",
               local_version.at("plugins"));
  return true;
}

} // namespace details

std::optional<std::chrono::steady_clock::time_point>
get_deadline(caf::timespan timeout) {
  if (caf::is_infinite(timeout))
    return {};
  return {std::chrono::steady_clock::now() + timeout};
}

std::optional<caf::timespan> get_retry_delay(const caf::settings& settings) {
  auto retry_delay
    = caf::get_or<caf::timespan>(settings, "tenzir.connection-retry-delay",
                                 defaults::node_connection_retry_delay);
  if (retry_delay == caf::timespan::zero())
    return {};
  return retry_delay;
}

caf::expected<endpoint> get_node_endpoint(const caf::settings& opts) {
  endpoint node_endpoint;
  auto endpoint_str
    = get_or(opts, "tenzir.endpoint", defaults::endpoint.data());
  if (!parsers::endpoint(endpoint_str, node_endpoint))
    return caf::make_error(ec::parse_error, "invalid endpoint",
                           endpoint_str.data());
  // Default to port 5158/tcp if none is set.
  if (!node_endpoint.port)
    node_endpoint.port = port{defaults::endpoint_port, port_type::tcp};
  if (node_endpoint.port->type() == port_type::unknown)
    node_endpoint.port->type(port_type::tcp);
  if (node_endpoint.port->type() != port_type::tcp)
    return caf::make_error(ec::invalid_configuration, "invalid protocol",
                           *node_endpoint.port);
  if (node_endpoint.host.empty())
    node_endpoint.host = defaults::endpoint_host;
  return node_endpoint;
}

caf::expected<node_actor> connect_to_node(caf::scoped_actor& self) {
  // If we already are in a node, do nothing.
  if (auto node = self->system().registry().get<node_actor>("tenzir.node")) {
    return node;
  }
  // Fetch values from config.
  const auto& opts = content(self->system().config());
  auto node_endpoint = details::get_node_endpoint(opts);
  if (!node_endpoint)
    return std::move(node_endpoint.error());
  auto timeout = node_connection_timeout(opts);
  auto connector_actor = self->spawn(connector, details::get_retry_delay(opts),
                                     details::get_deadline(timeout));
  auto result = caf::expected<node_actor>{caf::error{}};
  // `get_node_endpoint()` will add a default value.
  TENZIR_ASSERT(node_endpoint->port.has_value());
  self
    ->request(connector_actor, caf::infinite, atom::connect_v,
              connect_request{node_endpoint->port->number(),
                              node_endpoint->host})
    .receive(
      [&](node_actor& res) {
        result = std::move(res);
      },
      [&](caf::error& err) {
        result = std::move(err);
      });
  if (not result) {
    return result;
  }
  if (not *result) {
    return caf::make_error(ec::system_error,
                           "failed to connect to node: handle is invalid");
  }
  self->request(*result, timeout, atom::get_v, atom::version_v)
    .receive(
      [&](record& remote_version) {
        // TODO
        (void)details::check_version(remote_version);
      },
      [&](caf::error& error) {
        result = caf::make_error(ec::version_error,
                                 fmt::format("failed to receive remote version "
                                             "within specified "
                                             "connection-timeout of {}: {}",
                                             timeout, std::move(error)));
      });
  return result;
}

} // namespace tenzir
