//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/data.hpp"
#include "tenzir/http_api.hpp"
#include "tenzir/plugin/base.hpp"

#include <string>
#include <vector>

namespace tenzir {

// -- rest endpoint plugin -----------------------------------------------------

// A rest endpoint plugin declares a set of routes on which it can respond
// to HTTP requests, together with a `handler` actor that is responsible
// for doing that. A server (usually the `web` plugin) can then accept
// incoming requests and dispatch them to the correct handler according to the
// request path.
class rest_endpoint_plugin : public virtual plugin {
public:
  /// OpenAPI description of the plugin endpoints.
  /// @returns A record containing entries for the `paths` element of an
  ///          OpenAPI spec.
  [[nodiscard]] virtual auto
  openapi_endpoints(api_version version = api_version::latest) const -> record
    = 0;

  /// OpenAPI description of the schemas used by the plugin endpoints, if any.
  /// @returns A record containing entries for the `schemas` element of an
  ///          OpenAPI spec. The record may be empty if the plugin defines
  ///          no custom schemas.
  [[nodiscard]] virtual auto
  openapi_schemas(api_version /*version*/ = api_version::latest) const
    -> record {
    return record{};
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] virtual auto rest_endpoints() const
    -> const std::vector<rest_endpoint>& = 0;

  /// Actor that will handle this endpoint.
  //  TODO: This should get some integration with component_plugin so that
  //  the component can be used to answer requests directly.
  [[nodiscard]] virtual auto
  handler(caf::actor_system& system, node_actor node) const
    -> rest_handler_actor
    = 0;
};

} // namespace tenzir
