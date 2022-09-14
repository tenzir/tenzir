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
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/table_slice.hpp>

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::query {

// clang-format off

/// An actor to help with handling a single query.
using query_manager_actor = system::typed_actor_fwd<
    caf::reacts_to<atom::done>
  >
  ::extend_with<system::receiver_actor<table_slice>>
  ::unwrap;

/// An actor to receive REST endpoint requests and spawn exporters
/// as needed.
using request_multiplexer_actor = system::typed_actor_fwd<>
  ::extend_with<system::rest_handler_actor>
  ::unwrap;

// clang-format on

struct query_manager_state {
  query_manager_state() = default;

  system::index_actor index_ = {};
  size_t events_ = 0;
  size_t limit_ = std::string::npos;
  std::optional<system::query_cursor> cursor_ = std::nullopt;
  http_request request_;
};

struct request_multiplexer_state {
  request_multiplexer_state() = default;

  system::index_actor index_ = {};
};

query_manager_actor::behavior_type
query_manager(query_manager_actor::stateful_pointer<query_manager_state> self,
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
  return {// Index-facing API
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
            self->state.events_ += std::min(slice.rows(), remaining);
            // FIXME: avoid copy and preserve newlines
            auto raw_string
              = static_cast<std::stringstream&>(writer.out()).str();
            for (auto& c : raw_string)
              if (c == '\n')
                c = ',';
            // TODO: Use chunked response so we can send each slice immediately.
            self->state.request_.response->append(raw_string);
          },
          [self](atom::done) {
            bool remaining_partitions
              = self->state.cursor_->candidate_partitions
                > self->state.cursor_->scheduled_partitions;
            auto remaining_events = self->state.limit_ > self->state.events_;
            if (remaining_partitions && remaining_events) {
              auto next_batch_size = uint32_t{1};
              self->state.cursor_->scheduled_partitions += next_batch_size;
              self->send(self->state.index_, atom::query_v,
                         self->state.cursor_->id, next_batch_size);
            } else {
              auto footer = fmt::format("\nnull],  \"num_events\": {}\n}}\n",
                                        self->state.events_);
              self->state.request_.response->append(footer);
              self->state.request_.response.reset();
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
    [self](atom::http_request, uint64_t, http_request rq) {
      VAST_VERBOSE("{} handles /export request", *self);
      auto query_string = std::optional<std::string>{};
      if (rq.params.contains("expression")) {
        auto& param = rq.params.at("expression");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<std::string>(param));
        query_string = caf::get<std::string>(param);
      } else {
        // TODO: For the REST API this default feels a bit more dangerous than
        // for the CLI, since the user can not quickly notice the error and
        // abort with CTRL-C.
        query_string = "#type != \"this_expression_matches_everything\"";
      }
      auto expr = to<vast::expression>(*query_string);
      if (!expr) {
        rq.response->abort(400, "couldn't parse expression\n");
        return;
      }
      size_t limit = std::string::npos;
      if (rq.params.contains("limit")) {
        auto& param = rq.params.at("limit");
        // Should be type-checked by the server.
        VAST_ASSERT(caf::holds_alternative<count>(param));
        limit = caf::get<count>(param);
      }
      // TODO: Abort the request after some time limit has passed.
      auto exporter = self->spawn(query_manager, self->state.index_, *expr,
                                  limit, std::move(rq));
    },
  };
}

class plugin final : public virtual rest_endpoint_plugin {
  caf::error initialize([[maybe_unused]] data config) override {
    return {};
  }

  [[nodiscard]] const char* name() const override {
    return "api_export";
  };

  [[nodiscard]] std::string prefix() const override {
    return "";
  }

  [[nodiscard]] data openapi_specification(api_version version) const override {
    auto const* spec_v0 = R"_(
/query:
  post:
    summary: Create new query
    description: Create a new export query in VAST
    parameters:
      - in: query
        name: expression
        schema:
          type: string
          example: ":ip in 10.42.0.0/16"
        required: true
        description: Query string.
      - in: query
        name: lifetime
        schema:
          type: string
          example: "4 days"
        required: false
        default: "2 hours"
        description: How long to keep the query state alive.
    responses:
      200:
        description: Success.
        content: application/json
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

/query/{id}:
  get:
    summary: Get additional query results
    description: Return `n` additional results from the specified query.
    parameters:
      - in: path
        name: id
        schema:
          type: string
        required: true
        description: The query ID.
      - in: query
        name: n
        schema:
          type: integer
        required: false
        description: Maximum number of returned events
    responses:
      '200':
        description: Success.
        content: application/json
        schema:
          type: object
          properties:
            position:
              type: integer
              description: The number of events that had already been returned before this call.
            events:
              type: array
              items: object
      401:
        description: Not authenticated.
    )_";
    if (version != api_version::v0)
      return vast::record{};
    auto result = from_yaml(spec_v0);
    VAST_ASSERT(result);
    return *result;
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<rest_endpoint>&
  rest_endpoints() const override {
    static constexpr auto QUERY_ENDPOINT = 0;
    static constexpr auto QUERY_NEW_ENDPOINT = 0;
    static auto endpoints = std::vector<vast::rest_endpoint>{
    {
      .endpoint_id = QUERY_NEW_ENDPOINT,
      .method = http_method::post,
      .path = "/query",
      .params = vast::record_type{
        {"expression", vast::string_type{}},
        {"lifetime", vast::duration_type{}},
      },
      .version = api_version::v0,
      .content_type = http_content_type::json,
    },
    {
      .endpoint_id = QUERY_ENDPOINT,
      .method = http_method::get,
      .path = "/query/:id",
      .params = vast::record_type{
        {"n", vast::count_type{}},
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
