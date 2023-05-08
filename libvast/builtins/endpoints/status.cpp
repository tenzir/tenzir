//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>
#include <vast/system/builtin_rest_endpoints.hpp>
#include <vast/system/node.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::status {

static auto const* SPEC_V0 = R"_(
/status:
  get:
    summary: Return current status
    description: Returns the current status of the whole node.
    parameters:
      - in: query
        name: component
        schema:
          type: string
        required: false
        description: If specified, return the status for that component only.
        example: "index"
      - in: query
        name: verbosity
        schema:
          type: string
          enum: [info, detailed, debug]
          default: info
        required: false
        description: The verbosity level of the status response.
        example: detailed
    responses:
      200:
        description: OK.
        content:
          application/json:
            schema:
              type: object
            example:
              catalog:
                num-partitions: 7092
                memory-usage: 52781901584
              version:
                VAST: v2.3.0-rc3-32-g8529a6c43f
      401:
        description: Not authenticated.
    )_";

using status_handler_actor
  = system::typed_actor_fwd<>::extend_with<system::rest_handler_actor>::unwrap;

struct status_handler_state {
  static constexpr auto name = "status-handler";
  status_handler_state() = default;
  system::node_actor node_;
};

status_handler_actor::behavior_type
status_handler(status_handler_actor::stateful_pointer<status_handler_state> self,
               system::node_actor node) {
  self->state.node_ = std::move(node);
  return {
    [self](atom::http_request, uint64_t, http_request rq) {
      VAST_VERBOSE("{} handles /status request", *self);
      auto arguments = std::vector<std::string>{};
      if (rq.params.contains("component")) {
        auto& component = rq.params.at("component");
        // The server should have already type-checked this.
        VAST_ASSERT(caf::holds_alternative<std::string>(component));
        arguments.push_back(caf::get<std::string>(component));
      }
      auto options = caf::settings{};
      if (rq.params.contains("verbosity")) {
        auto verbosity = caf::get<std::string>(rq.params.at("verbosity"));
        if (verbosity == "info")
          /* nop */;
        else if (verbosity == "detailed")
          caf::put(options, "vast.status.detailed", true);
        else if (verbosity == "debug")
          caf::put(options, "vast.status.debug", true);
        else
          return rq.response->abort(422, "invalid verbosity\n", caf::error{});
      }
      auto inv = vast::invocation{
        .options = options,
        .full_name = "status",
        .arguments = arguments,
      };
      self
        ->request(self->state.node_, caf::infinite, atom::run_v, std::move(inv))
        .then(
          [rsp = rq.response](const caf::message&) {
            rsp->abort(500, "unexpected response\n", caf::error{});
          },
          [rsp = rq.response](caf::error& e) {
            // The NODE uses some black magic to respond to the request with
            // a `std::string`, which is not what its type signature says. This
            // will arrive as an "unexpected_response" error here.
            if (caf::sec{e.code()} != caf::sec::unexpected_response) {
              VAST_ERROR("node error {}", e);
              rsp->abort(500, "internal error\n", caf::error{});
              return;
            }
            std::string result;
            auto ctx = e.context();
            caf::message_handler{[&](caf::message& msg) {
              caf::message_handler{[&](std::string& str) {
                result = std::move(str);
              }}(msg);
            }}(ctx);
            rsp->append(result);
          });
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] const record& plugin_config,
                        [[maybe_unused]] const record& global_config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "api-status";
  };

  [[nodiscard]] std::string prefix() const override {
    return "";
  }

  [[nodiscard]] data openapi_specification(api_version version) const override {
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(SPEC_V0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<rest_endpoint>&
  rest_endpoints() const override {
    static const auto endpoints = std::vector<rest_endpoint>{
      {
        .endpoint_id = static_cast<uint64_t>(system::status_endpoints::status),
        .method = http_method::get,
        .path = "/status",
        .params = record_type{
          {"component", string_type{}},
          // TODO: Add direct support for `enumeration_type` to the server.
          {"verbosity", string_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  system::rest_handler_actor
  handler(caf::actor_system& system, system::node_actor node) const override {
    return system.spawn(status_handler, node);
  }
};

} // namespace vast::plugins::rest_api::status

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::status::plugin)
