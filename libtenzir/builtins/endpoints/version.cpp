//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/builtin_rest_endpoints.hpp>
#include <vast/node.hpp>
#include <vast/plugin.hpp>
#include <vast/version.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::status {

static auto const* SPEC_V0 = R"_(
/version:
  get:
    summary: Return node version
    description: Returns the version number of the node
    responses:
      200:
        description: OK.
        content:
          application/json:
            schema:
              type: object
            example:
              version: v2.3.0-rc3-32-g8529a6c43f
      401:
        description: Not authenticated.
    )_";

using status_handler_actor
  = typed_actor_fwd<>::extend_with<rest_handler_actor>::unwrap;

struct version_handler_state {
  static constexpr auto name = "version-handler";
  version_handler_state() = default;
};

auto version_handler(
  status_handler_actor::stateful_pointer<version_handler_state> self)
  -> status_handler_actor::behavior_type {
  return {
    [self](atom::http_request, uint64_t,
           const vast::record&) -> caf::result<rest_response> {
      VAST_DEBUG("{} handles /version request", *self);
      auto versions = retrieve_versions();
      VAST_ASSERT_CHEAP(versions.contains("Tenzir"));
      return rest_response{record{{{"version", versions.at("Tenzir")}}}};
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
    return "api-version";
  };

  [[nodiscard]] auto openapi_specification(api_version version) const
    -> data override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] auto rest_endpoints() const
    -> const std::vector<rest_endpoint>& override {
    static const auto endpoints = std::vector<rest_endpoint>{
      {
        .endpoint_id = static_cast<uint64_t>(status_endpoints::status),
        .method = http_method::post,
        .path = "/version",
        .params = std::nullopt,
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  auto handler(caf::actor_system& system, node_actor node) const
    -> rest_handler_actor override {
    return system.spawn(version_handler);
  }
};

} // namespace vast::plugins::rest_api::status

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::status::plugin)
