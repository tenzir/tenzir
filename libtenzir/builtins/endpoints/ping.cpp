//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/plugin.hpp>
#include <tenzir/version.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace tenzir::plugins::rest_api::ping {

static auto const* SPEC_V0 = R"_(
/ping:
  post:
    summary: Returns a success response
    description: Returns a success response to indicate that the node is able to respond to requests. The response body includes the current node version.
    responses:
      200:
        description: OK.
        content:
          application/json:
            schema:
              type: object
              properties:
                version:
                  type: string
                  description: The version of the responding node.
                  example: "v2.3.0-rc3-32-g8529a6c43f"
            example:
              version: v2.3.0-rc3-32-g8529a6c43f
      401:
        description: Not authenticated.
    )_";

using ping_handler_actor
  = typed_actor_fwd<>::extend_with<rest_handler_actor>::unwrap;

struct ping_handler_state {
  static constexpr auto name = "ping-handler";
  ping_handler_state() = default;
};

auto ping_handler(ping_handler_actor::stateful_pointer<ping_handler_state> self)
  -> ping_handler_actor::behavior_type {
  return {
    [self](atom::http_request, uint64_t,
           const tenzir::record&) -> caf::result<rest_response> {
      TENZIR_DEBUG("{} handles /ping request", *self);
      return rest_response{record{{{"version", tenzir::version::version}}}};
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "ping";
  };

  [[nodiscard]] auto openapi_endpoints(api_version version) const
    -> record override {
    if (version != api_version::v0)
      return tenzir::record{};
    auto result = from_yaml(SPEC_V0);
    TENZIR_ASSERT(result);
    TENZIR_ASSERT(is<record>(*result));
    return as<record>(*result);
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] auto rest_endpoints() const
    -> const std::vector<rest_endpoint>& override {
    static const auto endpoints = std::vector<rest_endpoint>{
      {
        .endpoint_id = static_cast<uint64_t>(0),
        .method = http_method::post,
        .path = "/ping",
        .params = std::nullopt,
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  auto handler(caf::actor_system& system, node_actor) const
    -> rest_handler_actor override {
    return system.spawn(ping_handler);
  }
};

} // namespace tenzir::plugins::rest_api::ping

TENZIR_REGISTER_PLUGIN(tenzir::plugins::rest_api::ping::plugin)
