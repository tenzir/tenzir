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
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/actors.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/spawn_arguments.hpp>
#include <vast/table_slice.hpp>

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::query {

static auto const* SPEC_V0 = R"_(
/query/new:
  post:
    summary: Create new query
    description: Create a new export query in VAST
    parameters:
      - in: query
        name: expression
        schema:
          type: string
          example: ":addr in 10.42.0.0/16"
        required: true
        description: Query string.
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              example:
                id: c91019bf-21fe-4999-8323-4d28aeb111ab
              properties:
                id:
                  type: string
      401:
        description: Not authenticated.
      422:
        description: Invalid expression or invalid lifetime.

/query/{id}/next:
  get:
    summary: Get additional query results
    description: Return `n` additional results from the specified query.
    parameters:
      - in: path
        name: id
        schema:
          type: string
        required: true
        example: e84308a2-1ba4-4559-9e0f-597dfea4fd3e
        description: The query ID.
      - in: query
        name: n
        schema:
          type: integer
        required: false
        example: 10
        description: Maximum number of returned events
    responses:
      200:
        description: Success.
        content:
          application/json:
            schema:
              type: object
              properties:
                position:
                  type: integer
                  description: The total number of events that has been returned by previous calls to this endpoint.
                events:
                  type: array
                  items:
                    type: object
                  description: The returned events.
              example:
                position: 20
                events:
                  - {"ts": "2009-11-18T22:11:04.011822", "uid": "iKxhjl8i1n3", "id.orig_h": "192.168.1.103"}
                  - {"ts": "2009-11-18T22:13:38.992072", "uid": "wsB2v2jcIXa", "id.orig_h": "192.168.1.103"}
      401:
        description: Not authenticated.
      422:
        description: Invalid arguments.
    )_";

static constexpr auto QUERY_NEW_ENDPOINT = 0;
static constexpr auto QUERY_NEXT_ENDPOINT = 1;

/// An actor to help with handling a single query.
using query_manager_actor = system::typed_actor_fwd<
  // Initiate a query.
  auto(atom::provision, system::query_cursor)->caf::result<void>,
  // Get the next results for a query.
  auto(atom::next, http_request, uint64_t)->caf::result<atom::done>,
  // Finish a query.
  auto(atom::done)->caf::result<void>>
  // Conform to the protocol of a RECEIVER ACTOR of table slices.
  ::extend_with<system::receiver_actor<table_slice>>::unwrap;

/// An actor to receive REST endpoint requests and spawn exporters
/// as needed.
using request_multiplexer_actor = system::typed_actor_fwd<>
  // Conform to the protocol of the REST HANDLER actor.
  ::extend_with<system::rest_handler_actor>::unwrap;

namespace {

// Remove up to `n` events from the `slices` vector and return them
// as a JSONLD string + number of events drained.
std::pair<size_t, std::string>
drain_buffer(std::deque<table_slice>& slices, size_t n) {
  auto written = size_t{0};
  bool error_encountered = false;
  auto ostream = std::make_unique<std::stringstream>();
  auto writer = vast::format::json::writer{std::move(ostream), caf::settings{}};
  for (auto it = slices.begin(); it != slices.end(); ++it) {
    auto& slice = *it;
    if (slice.rows() < n) {
      written += slice.rows();
      if (auto error = writer.write(slice))
        error_encountered = true;
    } else {
      auto [first, second] = split(slice, n);
      written += first.rows();
      if (auto error = writer.write(first))
        error_encountered = true;
      slice = second;
      slices.erase(slices.begin(), it);
      break;
    }
  }
  if (error_encountered)
    VAST_WARN("query endpoint encountered error writing json data");
  return {written, static_cast<std::stringstream&>(writer.out()).str()};
}

constexpr auto BATCH_SIZE = uint32_t{1};

} // namespace

struct query_manager_state {
  query_manager_state() = default;

  static constexpr auto name = "query_manager";

  bool query_in_progress = false;
  system::index_actor index = {};
  caf::typed_response_promise<atom::done> promise = {};
  http_request request;
  size_t position = 0;
  size_t events = 0;
  size_t limit = std::string::npos;
  std::string response_body; // The current response to the GET endpoint
  std::string body_buffer;   // JSONLD buildup of current response
  std::deque<table_slice> slice_buffer;
  std::optional<system::query_cursor> cursor = std::nullopt;
};

struct request_multiplexer_state {
  request_multiplexer_state() = default;

  static constexpr auto name = "request_multiplexer";

  system::index_actor index_ = {};
  std::unordered_map<std::string, query_manager_actor> live_queries_ = {};
};

query_manager_actor::behavior_type
query_manager(query_manager_actor::stateful_pointer<query_manager_state> self,
              system::index_actor index) {
  self->state.index = std::move(index);
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    if (self->state.promise.pending())
      self->state.promise.deliver(msg.reason);
  });
  return {[self](atom::provision, system::query_cursor cursor) {
            self->state.cursor = cursor;
          },
          [self](atom::next, http_request& rq,
                 uint64_t n) -> caf::result<atom::done> {
            if (!self->state.cursor)
              rq.response->abort(500, "query manager not ready");
            if (self->state.query_in_progress)
              rq.response->abort(503, "previous request still in progress");
            self->state.request = std::move(rq);
            self->state.limit = n;
            self->state.position += self->state.events;
            self->state.body_buffer = "";
            self->state.promise = self->make_response_promise<atom::done>();
            std::tie(self->state.events, self->state.body_buffer)
              = drain_buffer(self->state.slice_buffer, n);
            // Forward to the `done` handler to avoid repeating the same logic
            // here.
            self->send(self, atom::done_v);
            return self->state.promise;
          },
          // Index-facing API
          [self](const vast::table_slice& slice) {
            self->state.slice_buffer.push_back(slice);
            if (self->state.limit <= self->state.events)
              return;
            auto remaining = self->state.limit - self->state.events;
            auto [n, json] = drain_buffer(self->state.slice_buffer, remaining);
            self->state.body_buffer += json;
            self->state.events += n;
          },
          [self](atom::done) {
            // There's technically a race condition with atom::provision here,
            // since the index sends the first `done` asynchronously. But since
            // we always set `taste == 0`, we will not miss any data due to this.
            if (!self->state.cursor)
              return;
            if (!self->state.promise.pending())
              return;
            bool remaining_partitions
              = self->state.cursor->candidate_partitions
                > self->state.cursor->scheduled_partitions;
            auto remaining_events = self->state.limit > self->state.events;
            if (remaining_partitions && remaining_events) {
              self->state.cursor->scheduled_partitions += BATCH_SIZE;
              self->send(self->state.index, atom::query_v,
                         self->state.cursor->id, BATCH_SIZE);
            } else {
              self->state.response_body = fmt::format(
                "{{\"position\": {}, \"events\": [\n", self->state.position);
              std::istringstream iss{self->state.body_buffer};
              size_t count = 0ull;
              std::string line;
              while (std::getline(iss, line)) {
                if (count++ != 0)
                  self->state.response_body += ",\n";
                self->state.response_body += line;
              }
              self->state.response_body += "]}\n";
              auto request = std::exchange(self->state.request, {});
              request.response->append(self->state.response_body);
              self->state.promise.deliver(atom::done_v);
            }
          }};
}

request_multiplexer_actor::behavior_type request_multiplexer(
  request_multiplexer_actor::stateful_pointer<request_multiplexer_state> self,
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
    [self](atom::http_request, uint64_t endpoint_id, http_request rq) {
      VAST_VERBOSE("{} handles /query request", *self);
      if (endpoint_id == QUERY_NEW_ENDPOINT) {
        auto query_string = std::optional<std::string>{};
        if (rq.params.contains("expression")) {
          auto& param = rq.params.at("expression");
          // Should be type-checked by the server.
          VAST_ASSERT(caf::holds_alternative<std::string>(param));
          query_string = caf::get<std::string>(param);
        } else {
          return rq.response->abort(422, "missing parameter 'expression'\n");
        }
        auto expr = system::parse_expression(*query_string);
        if (!expr)
          return rq.response->abort(400, fmt::format("unparseable query: {}\n",
                                                     expr.error()));
        auto normalized_expr = normalize_and_validate(*expr);
        if (!normalized_expr)
          return rq.response->abort(400, fmt::format("invalid query: {}\n",
                                                     normalized_expr.error()));
        auto handler = self->spawn(query_manager, self->state.index_);
        auto query = vast::query_context::make_extract(
          "http-request", handler, std::move(*normalized_expr));
        query.taste = 0;
        self
          ->request(self->state.index_, caf::infinite, atom::evaluate_v, query)
          .then(
            [self, handler,
             response = rq.response](system::query_cursor cursor) {
              auto id_string = fmt::format("{:l}", cursor.id);
              self->state.live_queries_[id_string] = handler;
              self->send(handler, atom::provision_v, cursor);
              response->append(fmt::format("{{\"id\": \"{}\"}}\n", id_string));
            },
            [response = rq.response](const caf::error& e) {
              response->abort(500, fmt::format("received error response from "
                                               "index: {}\n",
                                               e));
            });
      } else {
        VAST_ASSERT_CHEAP(endpoint_id == QUERY_NEXT_ENDPOINT);
        if (!rq.params.contains("id"))
          return rq.response->abort(400, "missing id\n");
        if (!rq.params.contains("n"))
          return rq.response->abort(400, "missing parameter 'n'\n");
        auto id = caf::get<std::string>(rq.params["id"]);
        auto n = caf::get<count>(rq.params["n"]);
        auto it = self->state.live_queries_.find(id);
        if (it == self->state.live_queries_.end())
          return rq.response->abort(422, "unknown id\n");
        auto& handler = it->second;
        self->request(handler, caf::infinite, atom::next_v, std::move(rq), n)
          .then([](atom::done) { /* nop */ },
                [response = rq.response](const caf::error& e) {
                  response->abort(
                    500, fmt::format("internal server error: {}\n", e));
                });
      }
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] std::string name() const override {
    return "api-query";
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
        .endpoint_id = QUERY_NEW_ENDPOINT,
        .method = http_method::post,
        .path = "/query/new",
        .params = vast::record_type{
          {"expression", vast::string_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
      {
        .endpoint_id = QUERY_NEXT_ENDPOINT,
        .method = http_method::get,
        .path = "/query/:id/next",
        .params = vast::record_type{
          {"id", vast::string_type{}},
          {"n", vast::uint64_type{}},
        },
        .version = api_version::v0,
        .content_type = http_content_type::json,
      },
    };
    return endpoints;
  }

  system::rest_handler_actor
  handler(caf::actor_system& system, system::node_actor node) const override {
    return system.spawn(request_multiplexer, node);
  }
};

} // namespace vast::plugins::rest_api::query

VAST_REGISTER_PLUGIN(vast::plugins::rest_api::query::plugin)
