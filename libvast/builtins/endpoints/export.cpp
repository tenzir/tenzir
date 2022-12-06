//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/format/json.hpp"

#include <vast/command.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/db_version.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/catalog.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/table_slice.hpp>

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::export_ {

static auto const* SPEC_V0 = R"_(
/export:
  get:
    summary: Export data
    description: Export data from VAST according to a query. The query must be a valid expression in the VAST Query Language. (see https://vast.io/docs/understand/query-language)
    parameters:
      - in: query
        name: expression
        schema:
          type: string
        required: true
        default: A query matching every event.
        description: The query expression to execute.
        example: ":addr in 10.42.0.0/16"
      - in: query
        name: limit
        schema:
          type: integer
        required: false
        default: 50
        description: Maximum number of returned events
        example: 3
    responses:
      200:
        description: The result data.
        content:
          application/json:
            schema:
                type: object
                properties:
                  num_events:
                    type: integer
                  version:
                    type: string
                  events:
                    type: array
                    items: object
                example:
                  version: v2.3.0-169-ge42a9652e5-dirty
                  events:
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                    - "{\"timestamp\": \"2011-08-12T13:00:36.378914\", \"flow_id\": 269421754201300, \"pcap_cnt\": 22569, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 1027, \"dest_ip\": \"74.125.232.202\", \"dest_port\": 80, \"proto\": \"TCP\", \"event_type\": \"http\", \"community_id\": null, \"http\": {\"hostname\": \"cr-tools.clients.google.com\", \"url\": \"/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202\", \"http_port\": null, \"http_user_agent\": \"Google Update/1.3.21.65;winhttp\", \"http_content_type\": null, \"http_method\": \"GET\", \"http_refer\": null, \"protocol\": \"HTTP/1.1\", \"status\": null, \"redirect\": null, \"length\": 0}, \"tx_id\": 0}"
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                  num_events: 3
      401:
        description: Not authenticated.
      422:
        description: Invalid query string or invalid limit.

  post:
    summary: Export data
    description: Export data from VAST according to a query. The query must be a valid expression in the VAST Query Language. (see https://vast.io/docs/understand/query-language)
    requestBody:
      description: Request parameters
      required: false
      content:
        application/json:
          schema:
            type: object
            properties:
              expression:
                type: string
              limit:
                type: integer
    responses:
      200:
        description: The result data.
        content:
          application/json:
            schema:
                type: object
                properties:
                  num_events:
                    type: integer
                  version:
                    type: string
                  events:
                    type: array
                    items: object
                example:
                  version: v2.3.0-169-ge42a9652e5-dirty
                  events:
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                    - "{\"timestamp\": \"2011-08-12T13:00:36.378914\", \"flow_id\": 269421754201300, \"pcap_cnt\": 22569, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 1027, \"dest_ip\": \"74.125.232.202\", \"dest_port\": 80, \"proto\": \"TCP\", \"event_type\": \"http\", \"community_id\": null, \"http\": {\"hostname\": \"cr-tools.clients.google.com\", \"url\": \"/service/check2?appid=%7B430FD4D0-B729-4F61-AA34-91526481799D%7D&appversion=1.3.21.65&applang=&machine=0&version=1.3.21.65&osversion=5.1&servicepack=Service%20Pack%202\", \"http_port\": null, \"http_user_agent\": \"Google Update/1.3.21.65;winhttp\", \"http_content_type\": null, \"http_method\": \"GET\", \"http_refer\": null, \"protocol\": \"HTTP/1.1\", \"status\": null, \"redirect\": null, \"length\": 0}, \"tx_id\": 0}"
                    - "{\"timestamp\": \"2011-08-14T05:38:55.549713\", \"flow_id\": 929669869939483, \"pcap_cnt\": null, \"vlan\": null, \"in_iface\": null, \"src_ip\": \"147.32.84.165\", \"src_port\": 138, \"dest_ip\": \"147.32.84.255\", \"dest_port\": 138, \"proto\": \"UDP\", \"event_type\": \"netflow\", \"community_id\": null, \"netflow\": {\"pkts\": 2, \"bytes\": 486, \"start\": \"2011-08-12T12:53:47.928539\", \"end\": \"2011-08-12T12:53:47.928552\", \"age\": 0}, \"app_proto\": \"failed\"}"
                  num_events: 3
      401:
        description: Not authenticated.
      422:
        description: Invalid query string or invalid limit.
    )_";

/// The EXPORT_HELPER handles a single query request.
using export_helper_actor = system::typed_actor_fwd<
  // Receives an `atom::done` from the index after each batch of table slices.
  caf::reacts_to<atom::done>>
  // Receives table slices from the index.
  ::extend_with<system::receiver_actor<table_slice>>::unwrap;

/// The EXPORT_MULTIPLEXER receives requests against the rest api
/// and spawns export helper actors as needed.
using export_multiplexer_actor = system::typed_actor_fwd<>
  // Provide the REST HANDLER actor interface.
  ::extend_with<system::rest_handler_actor>::unwrap;

struct export_helper_state {
  export_helper_state() = default;

  system::index_actor index_ = {};
  size_t events_ = 0;
  size_t limit_ = std::string::npos;
  std::optional<system::query_cursor> cursor_ = std::nullopt;
  std::string stringified_events_;
  http_request request_;
};

struct export_multiplexer_state {
  export_multiplexer_state() = default;

  system::index_actor index_ = {};
};

export_helper_actor::behavior_type
export_helper(export_helper_actor::stateful_pointer<export_helper_state> self,
              system::index_actor index, vast::expression expr, size_t limit,
              http_request&& request) {
  self->state.index_ = std::move(index);
  self->state.request_ = std::move(request);
  self->state.limit_ = limit;
  auto initial_response = fmt::format(
    "{{\n  \"version\": \"{}\",\n  \"events\": [\n", vast::version::version);
  self->state.request_.response->append(initial_response);
  auto query = vast::query_context::make_extract("api", self, std::move(expr));
  self->request(self->state.index_, caf::infinite, atom::evaluate_v, query)
    .await(
      [self](system::query_cursor cursor) {
        self->state.cursor_ = cursor;
      },
      [self](const caf::error& e) {
        auto response = fmt::format("received error response from index {}", e);
        self->state.request_.response->abort(500, response);
        self->quit(e);
      });
  return {
    // Index-facing API
    [self](const vast::table_slice& slice) {
      if (self->state.limit_ <= self->state.events_)
        return;
      auto remaining = self->state.limit_ - self->state.events_;
      auto ostream = std::make_unique<std::stringstream>();
      auto writer
        = vast::format::json::writer{std::move(ostream), caf::settings{}};
      if (slice.rows() < remaining)
        writer.write(slice);
      else
        writer.write(truncate(slice, remaining));
      self->state.events_ += std::min<size_t>(slice.rows(), remaining);
      self->state.stringified_events_
        += static_cast<std::stringstream&>(writer.out()).str();
    },
    [self](atom::done) {
      bool remaining_partitions = self->state.cursor_->candidate_partitions
                                  > self->state.cursor_->scheduled_partitions;
      auto remaining_events = self->state.limit_ > self->state.events_;
      if (remaining_partitions && remaining_events) {
        auto next_batch_size = uint32_t{1};
        self->state.cursor_->scheduled_partitions += next_batch_size;
        self->send(self->state.index_, atom::query_v, self->state.cursor_->id,
                   next_batch_size);
      } else {
        auto& events_string = self->state.stringified_events_;
        // Remove line breaks since the answer isn't LDJSON, except for the
        // last since JSON doesn't support trailing commas.
        std::replace(events_string.begin(), events_string.end(), '\n', ',');
        if (!events_string.empty())
          events_string.back() = ' ';
        self->state.request_.response->append(std::move(events_string));
        auto footer
          = fmt::format("],\n  \"num_events\": {}\n}}\n", self->state.events_);
        self->state.request_.response->append(footer);
        self->state.request_.response.reset();
      }
    }};
}

export_multiplexer_actor::behavior_type export_multiplexer(
  export_multiplexer_actor::stateful_pointer<export_multiplexer_state> self,
  const system::node_actor& node) {
  self
    ->request(node, caf::infinite, atom::get_v, atom::label_v,
              std::vector<std::string>{"index"})
    .await(
      [self](std::vector<caf::actor>& components) {
        VAST_ASSERT_CHEAP(components.size() == 1);
        self->state.index_
          = caf::actor_cast<system::index_actor>(components[0]);
      },
      [self](caf::error& err) {
        VAST_ERROR("failed to get index from node: {}", std::move(err));
        self->quit();
      });
  return {
    [self](atom::http_request, uint64_t, http_request rq) {
      VAST_VERBOSE("{} handles /export request", *self);
      auto query_string = std::optional<std::string>{};
      if (rq.params.contains("expression")) {
        auto& param = rq.params.at("expression");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<std::string>(param));
        query_string = caf::get<std::string>(param);
      } else {
        query_string = "#type != \"this_expression_matches_everything\"";
      }
      auto expr = to<vast::expression>(*query_string);
      if (!expr) {
        rq.response->abort(400, "couldn't parse expression\n");
        return;
      }
      constexpr size_t DEFAULT_EXPORT_LIMIT = 50;
      size_t limit = DEFAULT_EXPORT_LIMIT;
      if (rq.params.contains("limit")) {
        auto& param = rq.params.at("limit");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<count>(param));
        limit = caf::get<count>(param);
      }
      // TODO: Abort the request after some time limit has passed.
      auto exporter = self->spawn(export_helper, self->state.index_, *expr,
                                  limit, std::move(rq));
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "api-export";
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
    static auto endpoints = std::vector<vast::rest_endpoint>{
    {
      .method = http_method::get,
      .path = "/export",
      .params = vast::record_type{
        {"expression", vast::string_type{}},
        {"limit", vast::count_type{}},
      },
      .version = api_version::v0,
      .content_type = http_content_type::json,
    },
    {
      .method = http_method::post,
      .path = "/export",
      .params = vast::record_type{
        {"expression", vast::string_type{}},
        {"limit", vast::count_type{}},
      },
      .version = api_version::v0,
      .content_type = http_content_type::json,
    },
  };
    return endpoints;
  }

  system::rest_handler_actor
  handler(caf::actor_system& system, system::node_actor node) const override {
    return system.spawn(export_multiplexer, node);
  }
};

} // namespace vast::plugins::rest_api::export_

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::export_::plugin)
