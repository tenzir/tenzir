//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/server_command.hpp"

#include "rest/handler_actors.hpp"
#include "vast/concept/parseable/numeric.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/format/json.hpp>
#include <vast/logger.hpp>
#include <vast/query.hpp>
#include <vast/system/node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <rest/server_command.hpp>
#include <restinio/all.hpp>

namespace vast::plugins::rest {

static restinio::request_handling_status_t
invalid_argument(restinio_request& rq, const std::string& reason) {
  auto rsp = rq->create_response(restinio::status_precondition_failed());
  rsp.set_body(reason);
  rsp.done();
  // rsp.connection_close();
  return restinio::request_accepted();
}

auto server_command(const vast::invocation& inv, caf::actor_system& system)
  -> caf::message {
  VAST_INFO("listening on http://localhost:8080");
  auto self = caf::scoped_actor{system};
  // Get VAST node.
  auto node_opt = vast::system::spawn_or_connect_to_node(
    self, inv.options, content(system.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  auto status_actor = self->spawn(status_handler, node);
  auto maybe_index
    = system::get_node_components<system::index_actor>(self, node);
  if (!maybe_index)
    return caf::make_message(std::move(maybe_index.error()));
  auto [index_] = *maybe_index;
  auto index = index_;
  // Set up routes.
  auto router = std::make_unique<restinio::router::express_router_t<>>();
  router->non_matched_request_handler([](auto req) {
    VAST_INFO("not found: {}", req->header().path());
    return req->create_response(restinio::status_not_found())
      .connection_close()
      .done();
  });
  router->http_get(
    "/", [](auto req, auto) -> restinio::request_handling_status_t {
      auto rsp = req->create_response(restinio::status_temporary_redirect());
      rsp.header().add_field("Location", fmt::format("/api/v1/status"));
      rsp.done();
      return restinio::request_accepted();
    });
  router->http_get(R"(/api/v1/status)",
                   [&self, status_actor](auto req, auto /*params*/)
                     -> restinio::request_handling_status_t {
                     self->send(status_actor, atom::request_v, atom::status_v,
                                request{std::move(req)});
                     return restinio::request_accepted();
                   });
  // Can't use the query id from the index, because we only learn
  // that together with the first partitions.
  size_t query_id_counter = 0ull;
  std::unordered_map<size_t, export_handler_actor> live_queries;
  router->http_get(
    R"(/export)",
    [](auto request, auto /*params*/) -> restinio::request_handling_status_t {
      return request->create_response()
        .append_header(restinio::http_field::server, "VAST Export Interface")
        .append_header_date_field()
        .append_header(restinio::http_field::content_type, "text/html; "
                                                           "charset=utf-8")
        .set_body(restinio::sendfile("../plugins/rest/www/query.html"))
        .done();
    });
  router->http_get(
    R"(/api/v1/export)",
    [&self, &query_id_counter, &live_queries, index](
      auto request, auto /*params*/) -> restinio::request_handling_status_t {
      const auto qp = restinio::parse_query(request->header().query());
      auto query_string = opt_value<std::string>(qp, "query");
      if (!query_string)
        query_string = "#type != \"this_expression_matches_everything\"";
      auto expr = to<vast::expression>(*query_string);
      if (!expr) {
        return invalid_argument(request, "couldn't parse expression");
      }
      auto query_id = ++query_id_counter;
      // We don't get the query_id that a result slice belongs to,
      // so we have to spawn a separate actor per request.
      auto exporter = self->spawn(export_handler, index);
      live_queries[query_id] = exporter;
      auto query = vast::query::make_extract(
        exporter, query::extract::mode::drop_ids, *expr);
      self->request(index, caf::infinite, atom::evaluate_v, query)
        .receive(
          [&self, exporter](system::query_cursor cursor) {
            self->send(exporter, cursor);
          },
          [=, &live_queries](const caf::error& e) {
            VAST_ERROR("received error response from index {}", e);
            live_queries.erase(query_id);
          });
      // FIXME: Just return the query id here, and make a user-facing `/query`
      // endpoint that displays the results and dynamically loads more.
      auto rsp
        = request->create_response(restinio::status_temporary_redirect());
      rsp.header().add_field("Location",
                             fmt::format("/api/v1/query/{}", query_id));
      rsp.done();
      return restinio::request_accepted();
    });
  router->http_get(
    R"(/api/v1/query/:id)",
    [&](auto rq, auto params) -> restinio::request_handling_status_t {
      auto id = restinio::cast_to<int>(params["id"]);
      auto it = live_queries.find(id);
      if (it == live_queries.end())
        return invalid_argument(rq, "unknown id");
      auto exporter = it->second;
      self->send(exporter, atom::request_v, atom::query_v, request{rq});
      return restinio::request_accepted();
    });
  router->http_get(R"(/api/v1/query/:id/next)",
                   [&](auto rq,
                       auto params) -> restinio::request_handling_status_t {
                     auto id = restinio::cast_to<int>(params["id"]);
                     auto it = live_queries.find(id);
                     if (it == live_queries.end())
                       return invalid_argument(rq, "unknown id");
                     auto exporter = it->second;
                     self->send(exporter, atom::request_v, atom::query_v,
                                atom::next_v, request{rq});
                     return restinio::request_accepted();
                   });
  // Run server.
  struct my_server_traits : public restinio::default_single_thread_traits_t {
    using request_handler_t = restinio::router::express_router_t<>;
  };
  restinio::run(restinio::on_this_thread<my_server_traits>()
                  .port(8080)
                  .address("localhost")
                  .request_handler(std::move(router)));
  // FIXME: Kill spawned actors.
  return {};
}

} // namespace vast::plugins::rest
