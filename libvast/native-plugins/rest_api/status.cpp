//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/plugin.hpp>
#include <vast/system/node.hpp>

#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::status {

using status_handler_actor
  = system::typed_actor_fwd<>::extend_with<system::rest_handler_actor>::unwrap;

struct status_handler_state {
  status_handler_state() = default;
  system::node_actor node_;
  std::vector<http_request> pending_;
};

status_handler_actor::behavior_type
status_handler(status_handler_actor::stateful_pointer<status_handler_state> self,
               system::node_actor node) {
  self->state.node_ = std::move(node);
  return {
    [self](atom::http_request, uint64_t, http_request rq) {
      VAST_VERBOSE("{} handles /status request", *self);
      auto request_in_progress = !self->state.pending_.empty();
      self->state.pending_.emplace_back(std::move(rq));
      if (request_in_progress)
        return;
      auto inv = vast::invocation{
        .options = {},
        .full_name = "status",
        .arguments = {},
      };
      self
        ->request(self->state.node_, caf::infinite, atom::run_v, std::move(inv))
        .then(
          [self](const caf::message&) {
            for (auto& rq : std::exchange(self->state.pending_, {}))
              rq.response->abort(500, "unexpected response");
          },
          [self](caf::error& e) {
            // The NODE uses some black magic to respond to the request with
            // a `std::string`, which is not what its type signature says. This
            // will arrive as an "unexpected_response" error here.
            if (caf::sec{e.code()} != caf::sec::unexpected_response) {
              for (auto& rq : std::exchange(self->state.pending_, {}))
                rq.response->abort(500, "internal error");
              return;
            }
            std::string result;
            caf::message_handler{[&](std::string& str) {
              result = std::move(str);
            }}(e.context());
            for (auto& rq : std::exchange(self->state.pending_, {}))
              rq.response->append(result);
          });
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "api_status";
  };

  [[nodiscard]] std::string prefix() const override {
    return "";
  }

  [[nodiscard]] data openapi_specification(api_version version) const override {
    static auto const* spec_v0 = R"_(
/status:
  get:
    summary: Returns current status
    description: Returns the current status of the whole node.
    responses:
      '200':
        description: A JSON dictionary with various pieces of info per component.
        content:
          application/json:
            schema:
              type: dict
    )_";
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(spec_v0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<api_endpoint>&
  api_endpoints() const override {
    static const auto endpoints = std::vector<api_endpoint>{
      {
        .method = http_method::get,
        .path = "/status",
        .params = caf::none,
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
