//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/server_command.hpp"

#include "web/authenticator.hpp"
#include "web/configuration.hpp"
#include "web/mime.hpp"
#include "web/restinio_response.hpp"
#include "web/restinio_server.hpp"
#include "web/specification_command.hpp"

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/data.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/format/json.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/validate.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <restinio/all.hpp>
#include <restinio/message_builders.hpp>
#include <restinio/request_handler.hpp>
#include <restinio/tls.hpp>
#include <restinio/websocket/websocket.hpp>

// Needed to forward incoming requests to the request_dispatcher

namespace vast::plugins::web {

using request_dispatcher_actor = system::typed_actor_fwd<
  // Handle a request.
  auto(atom::request, restinio_response_ptr, rest_endpoint,
       system::rest_handler_actor)
    ->caf::result<void>,
  // INTERNAL: Continue handling a requet.
  auto(atom::internal, atom::request, restinio_response_ptr, rest_endpoint,
       system::rest_handler_actor)
    ->caf::result<void>>::unwrap;

namespace {

restinio::http_method_id_t to_restinio_method(vast::http_method method) {
  switch (method) {
    case vast::http_method::get:
      return restinio::http_method_get();
    case vast::http_method::post:
      return restinio::http_method_post();
  }
  // Unreachable.
  return restinio::http_method_get();
}

auto parse_query_params(std::string_view text)
  -> caf::expected<restinio::query_string_params_t> {
  try {
    return restinio::parse_query(text);
  } catch (restinio::exception_t& e) {
    return caf::make_error(ec::parse_error, e.what());
  }
}

std::string
format_api_route(const rest_endpoint& endpoint, std::string_view prefix) {
  return fmt::format("/api/v{}{}{}", static_cast<uint8_t>(endpoint.version),
                     prefix, endpoint.path);
}

struct request_dispatcher_state {
  request_dispatcher_state() = default;

  web::server_config server_config;
  authenticator_actor authenticator;
};

request_dispatcher_actor::behavior_type request_dispatcher(
  request_dispatcher_actor::stateful_pointer<request_dispatcher_state> self,
  server_config config, authenticator_actor authenticator) {
  self->state.server_config = config;
  self->state.authenticator = authenticator;
  return {
    [self](atom::request, restinio_response_ptr& response,
           rest_endpoint& endpoint, system::rest_handler_actor handler) {
      // Skip authentication if its not required.
      if (!self->state.server_config.require_authentication) {
        self->send(self, atom::internal_v, atom::request_v, std::move(response),
                   std::move(endpoint), std::move(handler));
        return;
      }
      // Ask the authenticator to validate the passed token.
      auto const* token
        = response->request()->header().try_get_field("X-VAST-Token");
      if (!token) {
        response->abort(401, "missing header X-VAST-Token\n", caf::error{});
        return;
      }
      self
        ->request(self->state.authenticator, caf::infinite, atom::validate_v,
                  *token)
        .then(
          [self, response, endpoint = std::move(endpoint),
           handler](bool valid) mutable {
            if (valid)
              self->send(self, atom::internal_v, atom::request_v,
                         std::move(response), std::move(endpoint),
                         std::move(handler));
            else
              response->abort(401, "invalid token\n", caf::error{});
          },
          [response](const caf::error& err) mutable {
            response->abort(500, "authentication error\n", err);
          });
    },
    [self](atom::internal, atom::request, restinio_response_ptr& response,
           const rest_endpoint& endpoint, system::rest_handler_actor handler) {
      auto const& header = response->request()->header();
      auto query_params = parse_query_params(header.query());
      if (!query_params)
        return response->abort(400, "failed to parse query\n",
                               query_params.error());
      auto parser = simdjson::dom::parser{};
      auto body_params = std::optional<simdjson::dom::object>{};
      // POST requests can contain request parameters in their body in any
      // format supported by the server. The client indicates the data format
      // they used in the `Content-Type` header. See also
      // https://stackoverflow.com/a/26717908/92560
      if (header.method() == restinio::http_method_post()) {
        auto const& body = response->request()->body();
        auto maybe_content_type
          = header.opt_value_of(restinio::http_field_t::content_type);
        if (maybe_content_type == "application/x-www-form-urlencoded") {
          query_params = parse_query_params(body);
          if (!query_params)
            return response->abort(400, "failed to parse query\n",
                                   query_params.error());
        } else if (maybe_content_type == "application/json") {
          auto json_params = parser.parse(body);
          if (!json_params.is_object()) {
            return response->abort(
              400, "invalid JSON body\n",
              caf::make_error(ec::invalid_query,
                              simdjson::to_string(json_params)));
          }
          body_params = json_params.get_object();
        } else {
          return response->abort(
            400, "unsupported content type\n",
            caf::make_error(ec::invalid_argument,
                            fmt::format("{}\n", maybe_content_type)));
        }
      }
      auto const& route_params = response->route_params();
      auto combined_params = detail::stable_map<std::string, std::string>{};
      if (endpoint.params) {
        for (auto const& leaf : endpoint.params->leaves()) {
          auto name = leaf.field.name;
          // TODO: Warn and/or return an error if the same parameter
          // is passed using multiple methods.
          auto maybe_param = std::optional<std::string>{};
          if (auto query_param = query_params->get_param(name))
            maybe_param = std::string{*query_param};
          if (auto route_param = route_params.get_param(name))
            maybe_param = std::string{*route_param};
          if (body_params.has_value())
            if (auto maybe_value = body_params->at_key(name);
                maybe_value.error() != simdjson::NO_SUCH_FIELD) {
              maybe_param = simdjson::minify(maybe_value.value());
              if (maybe_value.is_string())
                maybe_param = detail::json_unescape(*maybe_param);
            }
          if (!maybe_param)
            continue;
          combined_params[name] = *maybe_param;
        }
      }
      auto params = parse_endpoint_parameters(endpoint, combined_params);
      if (!params)
        return response->abort(422, "failed to parse endpoint parametesr",
                               params.error());
      auto vast_request = vast::http_request{
        .params = std::move(*params),
        .response = std::move(response),
      };
      // Note that the handler should abort the response by itself if
      // possible (ie. invalid arguments), the error handler is to catch
      // timeouts and real internal errors.
      self
        ->request(handler, caf::infinite, atom::http_request_v,
                  endpoint.endpoint_id, std::move(vast_request))
        .then([]() { /*nop*/ },
              [](const caf::error& e) {
                VAST_WARN("internal server error while handling request: {}",
                          e);
              });
    },
  };
}

void setup_route(caf::scoped_actor& self, std::unique_ptr<router_t>& router,
                 std::string_view prefix, request_dispatcher_actor dispatcher,
                 const server_config& config, vast::rest_endpoint endpoint,
                 system::rest_handler_actor handler) {
  auto method = to_restinio_method(endpoint.method);
  auto path = format_api_route(endpoint, prefix);
  VAST_VERBOSE("setting up route {}", path);
  // The handler just injects the request into the actor system, the
  // actual processing starts in the request_dispatcher.
  router->add_handler(
    method, path,
    [=, &self](request_handle_t req,
               restinio::router::route_params_t route_params)
      -> restinio::request_handling_status_t {
      auto response = std::make_shared<restinio_response>(
        std::move(req), std::move(route_params), config.enable_detailed_errors,
        endpoint);
      if (config.cors_allowed_origin)
        response->add_header("Access-Control-Allow-Origin",
                             *config.cors_allowed_origin);
      for (auto const& [field, value] : config.response_headers)
        response->add_header(field, value);
      self->send(dispatcher, atom::request_v, std::move(response),
                 std::move(endpoint), handler);
      // TODO: Measure if always accepting introduces a noticeable
      // overhead and if so whether we can reject immediately in
      // some cases here.
      return restinio::request_accepted();
    });
}

// Set up a static handler that responds to all preflight requests
// with a 204 success response. We don't inspect the incoming path
// to be able to return a 404 error for non-existent paths. (instead
// of a CORS failure)
// cf. https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS
void setup_cors_preflight_handlers(std::unique_ptr<router_t>& router,
                                   const std::string& allowed_origin) {
  VAST_VERBOSE("allowing CORS requests from origin '{}'", allowed_origin);
  router->add_handler(
    restinio::http_method_options(), "/:path(.*)",
    [=](request_handle_t req, restinio::router::route_params_t)
      -> restinio::request_handling_status_t {
      auto const* requested_headers
        = req->header().try_get_field("Access-Control-Request-Headers");
      auto allowed_headers
        = requested_headers ? *requested_headers : "Content-Type";
      if (!requested_headers)
        return req->create_response(restinio::status_bad_request()).done();
      return req->create_response(restinio::status_no_content())
        .append_header("Access-Control-Allow-Origin", allowed_origin)
        .append_header("Access-Control-Allow-Methods", "POST, GET")
        .append_header("Access-Control-Allow-Headers", allowed_headers)
        .append_header("Access-Control-Max-Age", "86400")
        .done();
    });
}

} // namespace

auto server_command(const vast::invocation& inv, caf::actor_system& system)
  -> caf::message {
  auto self = caf::scoped_actor{system};
  auto web_options = caf::get_or(inv.options, "plugins.web", caf::settings{});
  auto data = vast::data{};
  // TODO: Implement a single `convert_and_validate()` function for going
  // from caf::settings -> record_type
  if (!inv.arguments.empty())
    return caf::make_message(caf::make_error(
      ec::invalid_argument,
      fmt::format("unexpected positional args: {}", inv.arguments)));
  bool success = convert(web_options, data);
  if (!success)
    return caf::make_message(
      caf::make_error(ec::invalid_argument, "couldnt parse options"));
  auto invalid
    = vast::validate(data, vast::plugins::web::configuration::schema(),
                     vast::validate::permissive);
  if (invalid)
    return caf::make_message(caf::make_error(
      ec::invalid_argument, fmt::format("invalid options: {}", invalid)));
  web::configuration config;
  caf::error error = convert(data, config);
  if (error)
    return caf::make_message(
      caf::make_error(ec::invalid_argument, "couldnt convert options"));
  auto server_config = convert_and_validate(config);
  if (!server_config) {
    VAST_ERROR("failed to start server: {}", server_config.error());
    return caf::make_message(caf::make_error(
      ec::invalid_configuration,
      fmt::format("invalid server configuration: {}", server_config.error())));
  }
  // Create necessary actors.
  auto node_opt = vast::system::spawn_or_connect_to_node(
    self, inv.options, content(system.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt)) {
    VAST_ERROR("failed to get node: {}", *err);
    return caf::make_message(std::move(*err));
  }
  const auto& node
    = std::holds_alternative<system::node_actor>(node_opt)
        ? std::get<system::node_actor>(node_opt)
        : std::get<scope_linked<system::node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  auto authenticator = get_authenticator(self, node, caf::infinite);
  if (!authenticator) {
    VAST_ERROR("failed to get web component: {}", authenticator.error());
    return caf::make_message(std::move(authenticator.error()));
  }
  auto dispatcher
    = self->spawn(request_dispatcher, *server_config, *authenticator);
  // Set up router.
  auto router = std::make_unique<router_t>();
  VAST_ASSERT_CHEAP(dispatcher);
  // Set up API routes from plugins.
  std::vector<system::rest_handler_actor> handlers;
  std::vector<std::string> api_routes;
  for (auto const* rest_plugin : plugins::get<rest_endpoint_plugin>()) {
    auto prefix = rest_plugin->prefix();
    if (!prefix.empty() && prefix[0] != '/') {
      VAST_WARN("ignoring endpoints with invalid prefix {}", prefix);
      continue;
    }
    auto handler = rest_plugin->handler(system, node);
    handlers.push_back(handler);
    for (auto const& endpoint : rest_plugin->rest_endpoints()) {
      if (endpoint.path.empty() || endpoint.path[0] != '/') {
        VAST_WARN("ignoring route {} due to missing '/'", endpoint.path);
        continue;
      }
      api_routes.push_back(format_api_route(endpoint, rest_plugin->prefix()));
      setup_route(self, router, rest_plugin->prefix(), dispatcher,
                  *server_config, std::move(endpoint), handler);
    }
    // TODO: Monitor the handlers and re-spawn them if they go down.
  }
  // Set up implicit CORS preflight handlers for all endpoints if desired
  if (server_config->cors_allowed_origin)
    setup_cors_preflight_handlers(router, *server_config->cors_allowed_origin);
  // Set up non-API routes.
  router->non_matched_request_handler([](auto req) {
    VAST_VERBOSE("404 not found: {} {}", req->header().method().c_str(),
                 req->header().path());
    return req->create_response(restinio::status_not_found())
      .set_body("404 not found\n")
      .done();
  });
  router->http_get(
    "/", [](auto request, auto) -> restinio::request_handling_status_t {
      return request->create_response(restinio::status_temporary_redirect())
        .append_header(restinio::http_field::server, "VAST")
        .append_header_date_field()
        .append_header(restinio::http_field::location, "/api/v0/status")
        .done();
    });
  auto openapi = generate_openapi_json();
  router->http_get(
    "/openapi.json",
    [openapi](auto request, auto) -> restinio::request_handling_status_t {
      return request->create_response(restinio::status_ok())
        .append_header(restinio::http_field::content_type, "application/json")
        .set_body(openapi)
        .done();
    });
  if (server_config->webroot) {
    VAST_VERBOSE("using {} as document root", *server_config->webroot);
    router->http_get(
      "/:path(.*)", restinio::path2regex::options_t{}.strict(true),
      [webroot = *server_config->webroot, api_routes](auto req,
                                                      auto /*params*/) {
        auto ec = std::error_code{};
        auto http_path = req->header().path();
        // Catch the common mistake of sending a GET request to a POST endpoint.
        if (http_path.starts_with("/api")
            && std::find(api_routes.begin(), api_routes.end(), http_path)
                 != api_routes.end())
          return req->create_response(restinio::status_not_found())
            .set_body("invalid request method\n")
            .done();
        auto path = std::filesystem::path{std::string{http_path}};
        VAST_DEBUG("serving static file {}", http_path);
        auto normalized_path
          = (webroot / path.relative_path()).lexically_normal();
        if (ec)
          return restinio::request_rejected();
        if (!normalized_path.string().starts_with(webroot.string()))
          return restinio::request_rejected();
        // Map e.g. /status -> /status.html on disk.
        if (!exists(normalized_path) && !normalized_path.has_extension())
          normalized_path.replace_extension("html");
        if (!exists(normalized_path))
          return req->create_response(restinio::status_not_found())
            .set_body("404 not found\n")
            .done();
        auto extension = normalized_path.extension().string();
        auto sf = restinio::sendfile(normalized_path);
        auto const* mime_type = content_type_by_file_extension(extension);
        return req->create_response()
          .append_header(restinio::http_field::server, "VAST")
          .append_header_date_field()
          .append_header(restinio::http_field::content_type, mime_type)
          .set_body(std::move(sf))
          .done();
      });
  } else {
    VAST_VERBOSE("not serving a document root because no --web-root was given "
                 "and the default location does not exist");
  }
  // Run server.
  auto io_context = asio::io_context{};
  auto server = make_server(*server_config, std::move(router),
                            restinio::external_io_context(io_context));
  // Post initial action to asio event loop. Note that the action
  // must have been posted *before* calling `io_context.run()`.
  asio::post(io_context, [&] {
    std::visit(detail::overload{[](auto& server) {
                 server->open_sync();
               }},
               server);
  });
  // Launch the thread on which the server will work.
  std::thread server_thread{[&] {
    auto const* scheme = server_config->require_tls ? "https" : "http";
    VAST_INFO("server listening on on {}://{}:{}", scheme,
              server_config->bind_address, server_config->port);
    io_context.run();
  }};
  // Run main loop.
  caf::error err;
  auto stop = false;
  self->monitor(node);
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        VAST_ASSERT(msg.source == node);
        VAST_DEBUG("{} received DOWN from node", *self);
        stop = true;
        if (msg.reason != caf::exit_reason::user_shutdown)
          err = std::move(msg.reason);
      },
      // Only called when running this command with `vast -N`.
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", detail::pretty_type_name(inv.full_name),
                   ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
        stop = true;
      })
    .until([&] {
      return stop;
    });
  // Shutdown
  std::visit(
    detail::overload{
      [](auto& server) {
        restinio::initiate_shutdown(*server);
      },
    },
    server);
  self->send_exit(dispatcher, caf::exit_reason::user_shutdown);
  for (auto& handler : handlers)
    self->send_exit(handler, caf::exit_reason::user_shutdown);
  server_thread.join();
  return caf::make_message(std::move(err));
}

} // namespace vast::plugins::web
