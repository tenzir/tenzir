//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
#include <vast/table_slice.hpp>

#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest_api::export_ {

// clang-format off

/// An actor to help with handling a single query.
using export_helper_actor = system::typed_actor_fwd<
    caf::reacts_to<atom::request, atom::query, http_request>,
    caf::reacts_to<atom::request, atom::query, atom::next, http_request>,
    caf::reacts_to<system::query_cursor>,
    caf::reacts_to<atom::done>
  >
  ::extend_with<system::receiver_actor<table_slice>>
  ::unwrap;

/// An actor to receive REST endpoint requests and spawn exporters
/// as needed.
using export_multiplexer_actor = system::typed_actor_fwd<>
  ::extend_with<system::rest_handler_actor>
  ::unwrap;

// clang-format on

struct export_helper_state {
  export_helper_state() = default;

  system::index_actor index_ = {};
  std::optional<system::query_cursor> cursor_ = std::nullopt;
  std::string body_ = {};
};

struct export_multiplexer_state {
  export_multiplexer_state() = default;

  system::index_actor index_ = {};
  size_t query_id_counter_ = {};
  std::unordered_map<size_t, export_helper_actor> live_queries_ = {};
};

export_helper_actor::behavior_type
export_helper(export_helper_actor::stateful_pointer<export_helper_state> self,
              system::index_actor index, vast::expression expr) {
  self->state.index_ = std::move(index);
  auto query = vast::query_context::make_extract("api", self, std::move(expr));
  self->request(self->state.index_, caf::infinite, atom::evaluate_v, query)
    .await(
      [self](system::query_cursor cursor) {
        self->send(self, cursor);
      },
      [self](const caf::error& e) {
        VAST_ERROR("received error response from index {}", e);
        self->quit(e);
      });
  return {// REST-facing API
          [self](atom::request, atom::query, http_request rq) {
            // TODO!
          },
          [self](atom::request, atom::query, atom::next, http_request rq) {
            // TODO!
          },
          // Index-facing API
          [self](system::query_cursor cursor) {
            self->state.cursor_ = cursor;
            self->send(self->state.index_, self->state.cursor_->id,
                       uint32_t{1});
          },
          [self](const vast::table_slice& slice) {
            // TODO: Use chunked response so we can send each slice immediately.
            self->state.body_ += fmt::format("{}\n", slice);
          },
          [self](atom::done) {
            // TODO: It would be better to wait until we get the `done`
            // to ensure the client gets a complete result back from
            // one GET request, but for some obscure reason the final
            // `done` doesn't seem to arrive here from the query supervisor.
            //
            // self->state.loading = false;
            // for (auto& rq : std::exchange(self->state.pending, {})) {
            //   auto rsp = rq.impl->create_response();
            //   rsp.set_body(self->state.body_);
            //   rsp.done();
            // }
          }};
}

// TODO: This is a bit silly, probably it makes more sense to just
// return a `map` from `plugin::endpoints()`?
constexpr uint64_t EXPORT_ENDPOINT = 0ull;
constexpr uint64_t QUERY_ENDPOINT = 1ull;
constexpr uint64_t QUERY_NEXT_ENDPOINT = 2ull;

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
      [self](caf::error& err) { //
        VAST_ERROR("failed to get index from node: {}", std::move(err));
        self->quit();
      });
  self->set_down_handler([=](const caf::down_msg& msg) {
    VAST_INFO("{} goes down", msg.source); // FIXME: INFO -> DEBUG
    // FIXME
    // auto it = self->state.live_queries_.find(msg.source);
    // if (msg.reason != caf::exit_reason::normal) {
    //   request->response->abort(500, "error");
    // }
  });
  return {
    [self](atom::http_request, uint64_t endpoint_id, http_request rq) {
      if (endpoint_id == EXPORT_ENDPOINT) {
        auto query_param = rq.params.at("query");
        auto query_string = std::optional<std::string>{};
        if (auto* input = caf::get_if<std::string>(&query_param.get_data())) {
          query_string = *input;
        } else {
          // TODO: For the REST API this default feels a bit more dangerous than
          // for the CLI, since the user can not quickly notice the error and
          // abort with CTRL-C.
          query_string = "#type != \"this_expression_matches_everything\"";
        }
        auto expr = to<vast::expression>(*query_string);
        if (!expr) {
          rq.response->abort(400, "couldn't parse expression");
          return;
        }
        auto query_id = ++self->state.query_id_counter_;
        auto exporter = self->spawn<caf::monitored>(export_helper,
                                                    self->state.index_, *expr);
        self->state.live_queries_[query_id] = exporter;
        rq.response->append(fmt::format("{}", query_id));
      } else if (endpoint_id == QUERY_ENDPOINT) {
        auto id_param = rq.params.at("id");
        auto const* maybe_id = caf::get_if<count>(&id_param.get_data());
        if (!maybe_id) {
          rq.response->abort(400, "invalid id");
          return;
        }
        auto const& id = *maybe_id;
        auto it = self->state.live_queries_.find(id);
        if (it == self->state.live_queries_.end()) {
          rq.response->abort(422, "unknown id");
          return;
        }
        auto export_helper = it->second;
        self->send(export_helper, atom::request_v, atom::query_v,
                   std::move(rq));
      } else if (endpoint_id == QUERY_NEXT_ENDPOINT) {
        auto id_param = rq.params.at("id");
        auto const* maybe_id = caf::get_if<count>(&id_param.get_data());
        if (!maybe_id) {
          rq.response->abort(400, "invalid id");
          return;
        }
        auto const& id = *maybe_id;
        auto it = self->state.live_queries_.find(id);
        if (it == self->state.live_queries_.end()) {
          rq.response->abort(422, "unknown id");
          return;
        }
        auto export_helper = it->second;
        self->send(export_helper, atom::request_v, atom::query_v, atom::next_v,
                   std::move(rq));
      } else {
        VAST_WARN("ignoring unknown request {}", endpoint_id);
      }
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

  /// OpenAPI documentation for the plugin endpoints.
  [[nodiscard]] std::string_view openapi_specification() const override {
    return R"_(
---
openapi: 3.0.0
paths:
  /export:
    post:
      summary: Start a new query
      description: Create a new export query in VAST
      responses:
        '200':
          description: Success.
  /query/{id}:
    get:
      summary: Get the current result set of the query.
  /query/{id}/next:
    get:
      summary: Fetch new results for the query and display them.
    )_";
  }

  /// List of API endpoints provided by this plugin.
  [[nodiscard]] const std::vector<api_endpoint>&
  api_endpoints() const override {
    static auto endpoints = std::vector<vast::rest_endpoint_plugin::api_endpoint>{
    {
      .endpoint_id = EXPORT_ENDPOINT,
      .method = http_method::post,
      .path = "/export",
      .params = vast::record_type{
        {"query", vast::string_type{}},
      },
      .version = rest_endpoint_plugin::api_version::v0,
      .content_type = http_content_type::json,
    },
    {
      .endpoint_id = QUERY_ENDPOINT,
      .method = http_method::get,
      .path = "/query/:id",
      .params = vast::record_type{
        {"id", vast::count_type{}},
      },
      .version = rest_endpoint_plugin::api_version::v0,
      .content_type = http_content_type::json,
    },
    {
      .endpoint_id = QUERY_NEXT_ENDPOINT,
      .method = http_method::get,
      .path = "/query/:id/next",
      .params = std::nullopt,
      .version = rest_endpoint_plugin::api_version::v0,
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
