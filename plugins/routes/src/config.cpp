//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors

#include "routes/config.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/view.hpp>

namespace tenzir::plugins::routes {

auto config::make(const view<record>& data, session ctx) -> failure_or<config> {
  auto result = config{};
  auto has_errors = false;
  for (const auto& [key, value] : data) {
    if (key == "connections") {
      const auto* connections_list = try_as<view<list>>(&value);
      if (not connections_list) {
        diagnostic::error("connections must be a list")
          .note("invalid config definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      for (const auto& connection_value : *connections_list) {
        const auto* connection_record = try_as<view<record>>(&connection_value);
        if (not connection_record) {
          diagnostic::error("connection entries must be records")
            .note("invalid config definition")
            .emit(ctx);
          has_errors = true;
          continue;
        }
        auto parsed_connection = connection::make(*connection_record, ctx);
        if (not parsed_connection) {
          has_errors = true;
        } else {
          result.connections.push_back(*parsed_connection);
        }
      }
      continue;
    }
    if (key == "routes") {
      const auto* routes_record = try_as<view<record>>(&value);
      if (not routes_record) {
        diagnostic::error("routes must be a record")
          .note("invalid config definition")
          .emit(ctx);
        has_errors = true;
        continue;
      }
      for (const auto& [route_name, route_value] : *routes_record) {
        const auto* route_record = try_as<view<record>>(&route_value);
        if (not route_record) {
          diagnostic::error("route entries must be records")
            .note("while parsing route {} in config definition", route_name)
            .emit(ctx);
          has_errors = true;
          continue;
        }
        auto parsed_route = route::make(*route_record, ctx);
        if (not parsed_route) {
          has_errors = true;
        } else {
          result.routes[std::string{route_name}] = *parsed_route;
        }
      }
      continue;
    }

    diagnostic::error("unknown field '{}'", key)
      .note("valid fields are: 'connections', 'routes'")
      .note("invalid config definition")
      .emit(ctx);
    has_errors = true;
  }

  // TODO: Add validation logic:
  // - Check that route inputs reference valid connections
  // - Check that route outputs reference valid connections
  // - Check for circular dependencies
  // - Validate that connection names are unique where required

  if (has_errors) {
    return failure::promise();
  }

  return result;
}

auto config::to_record() const -> record {
  auto result = record{};
  // Convert connections
  auto connections_list = list{};
  connections_list.reserve(connections.size());
  for (const auto& connection : connections) {
    connections_list.emplace_back(connection.to_record());
  }
  result["connections"] = std::move(connections_list);
  // Convert routes
  auto routes_record = record{};
  for (const auto& [name, route] : routes) {
    routes_record[name] = route.to_record();
  }
  result["routes"] = std::move(routes_record);
  return result;
}

} // namespace tenzir::plugins::routes
