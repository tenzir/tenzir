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

using router_t = restinio::router::express_router_t<>;

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
        response->abort(401, "missing header X-VAST-Token\n");
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
              response->abort(401, "invalid token\n");
          },
          [self, response](const caf::error& err) mutable {
            VAST_WARN("{} received error while authenticating request: {}",
                      *self, err);
            response->abort(500, "internal server error\n");
          });
    },
    [self](atom::internal, atom::request, restinio_response_ptr& response,
           const rest_endpoint& endpoint, system::rest_handler_actor handler) {
      auto const& header = response->request()->header();
      auto query_params = parse_query_params(header.query());
      if (!query_params)
        return response->abort(400, fmt::format("failed to parse query "
                                                "parameters: "
                                                "{}\n",
                                                query_params.error()));
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
            return response->abort(400, fmt::format("failed to parse query "
                                                    "parameters: "
                                                    "{}\n",
                                                    query_params.error()));
        } else if (maybe_content_type == "application/json") {
          auto json_params = parser.parse(body);
          if (!json_params.is_object())
            return response->abort(400, "invalid JSON body\n");
          body_params = json_params.get_object();
        } else {
          return response->abort(400, "unsupported content type");
        }
      }
      auto const& route_params = response->route_params();
      vast::record params;
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
                maybe_value.error() != simdjson::NO_SUCH_FIELD)
              maybe_param = simdjson::minify(maybe_value.value());
          if (!maybe_param)
            continue;
          auto string_value = *maybe_param;
          auto typed_value
            = caf::visit(
              detail::overload{
                [&string_value](const string_type&) -> caf::expected<data> {
                  return data{string_value};
                },
                [&string_value]<basic_type Type>(
                  const Type&) -> caf::expected<data> {
                  using data_t = type_to_data_t<Type>;
                  auto result = to<data_t>(string_value);
                  if (!result)
                    return result.error();
                  return *result;
                },
                []<complex_type Type>(const Type&) -> caf::expected<data> {
                  // TODO: Also allow lists.
                  return caf::make_error(ec::invalid_argument,
                                         "REST API only accepts basic type "
                                         "parameters");
                },
              },
              leaf.field.type);
          if (!typed_value)
            return response->abort(
              422, fmt::format("failed to parse parameter '{}'\n", name));
          params[name] = std::move(*typed_value);
        }
      }
      auto vast_request = vast::http_request{
        .params = std::move(params),
        .response = std::move(response),
      };
      self->send(handler, atom::http_request_v, endpoint.endpoint_id,
                 std::move(vast_request));
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
        std::move(req), std::move(route_params), endpoint);
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
    = vast::validate(data, vast::plugins::web::configuration::layout(),
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
    for (auto const& endpoint : rest_plugin->rest_endpoints()) {
      if (endpoint.path.empty() || endpoint.path[0] != '/') {
        VAST_WARN("ignoring route {} due to missing '/'", endpoint.path);
        continue;
      }
      api_routes.push_back(format_api_route(endpoint, rest_plugin->prefix()));
      handlers.push_back(handler);
      setup_route(self, router, rest_plugin->prefix(), dispatcher,
                  *server_config, std::move(endpoint), handler);
    }
  }
  // Set up non-API routes.
  router->non_matched_request_handler([](auto req) {
    VAST_VERBOSE("404 not found: {}", req->header().path());
    return req->create_response(restinio::status_not_found())
      .set_body("404 not found\n")
      .done();
  });
  router->http_get(
    "/", [](auto request, auto) -> restinio::request_handling_status_t {
      return request->create_response(restinio::status_temporary_redirect())
        .append_header(restinio::http_field::server, "VAST")
        .append_header_date_field()
        .append_header(restinio::http_field::location, "/index")
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
  if (!server_config->require_tls) {
    struct my_server_traits : public restinio::default_single_thread_traits_t {
      using request_handler_t = restinio::router::express_router_t<>;
    };
    VAST_INFO("web plugin listening on http://{}:{}", config.bind_address,
              config.port);
    restinio::run(restinio::on_this_thread<my_server_traits>()
                    .port(config.port)
                    .address(config.bind_address)
                    .request_handler(std::move(router)));
  } else {
    using traits_t = restinio::single_thread_tls_traits_t<
      restinio::asio_timer_manager_t, restinio::single_threaded_ostream_logger_t,
      restinio::router::express_router_t<>>;

    asio::ssl::context tls_context{asio::ssl::context::tls};
    // Most examples also set `asio::ssl::context::default_workarounds`, but
    // based on [1] these are only relevant for SSL which we don't support
    // anyways.
    // [1]: https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_set_options.html
    tls_context.set_options(asio::ssl::context::tls_server
                            | asio::ssl::context::single_dh_use);
    if (server_config->require_clientcerts)
      tls_context.set_verify_mode(
        asio::ssl::context::verify_peer
        | asio::ssl::context::verify_fail_if_no_peer_cert);
    else
      tls_context.set_verify_mode(asio::ssl::context::verify_none);
    tls_context.use_certificate_chain_file(config.certfile);
    tls_context.use_private_key_file(config.keyfile, asio::ssl::context::pem);
    // Manually specifying DH parameters is deprecated in favor of using
    // the OpenSSL built-in defaults, but asio has not been updated to
    // expose this API so we need to use the raw context.
    SSL_CTX_set_dh_auto(tls_context.native_handle(), true);
    VAST_INFO("web plugin listening on https://{}:{}", config.bind_address,
              config.port);
    using namespace std::literals::chrono_literals;
    restinio::run(restinio::on_this_thread<traits_t>()
                    .address(config.bind_address)
                    .port(config.port)
                    .request_handler(std::move(router))
                    .read_next_http_message_timelimit(10s)
                    .write_http_response_timelimit(1s)
                    .handle_request_timeout(1s)
                    .tls_context(std::move(tls_context)));
  }
  self->send_exit(dispatcher, caf::error{});
  for (auto& handler : handlers)
    self->send_exit(handler, caf::error{});
  return {};
}

} // namespace vast::plugins::web
