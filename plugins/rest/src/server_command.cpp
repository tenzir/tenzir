//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/server_command.hpp"

#include "rest/configuration.hpp"
#include "rest/restinio_response.hpp"
#include "rest/server_command.hpp"

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/parseable/numeric.hpp>
#include <vast/concept/parseable/to.hpp>
#include <vast/concept/parseable/vast/expression.hpp>
#include <vast/format/json.hpp>
#include <vast/logger.hpp>
#include <vast/plugin.hpp>
#include <vast/query_context.hpp>
#include <vast/system/node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/query_cursor.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <restinio/all.hpp>
#include <restinio/message_builders.hpp>
#include <restinio/request_handler.hpp>
#include <restinio/tls.hpp>
#include <restinio/websocket/websocket.hpp>

namespace vast::plugins::rest {

using router_t = restinio::router::express_router_t<>;
using auth_token = std::array<std::byte, 32>;

// FIXME: Don't use static variable for this.
static bool s_require_authentication = true;
static std::vector<std::string> s_tokens;

static restinio::http_method_id_t to_restinio_method(vast::http_method method) {
  switch (method) {
    case vast::http_method::get:
      return restinio::http_method_get();
    case vast::http_method::post:
      return restinio::http_method_post();
  }
  // Unreachable.
  return restinio::http_method_get();
}

static void
setup_route(caf::scoped_actor& self, std::unique_ptr<router_t>& router,
            std::string_view prefix,
            const vast::rest_endpoint_plugin::api_endpoint& endpoint,
            system::rest_handler_actor handler) {
  auto method = to_restinio_method(endpoint.method);
  auto path
    = fmt::format("/api/v{}{}{}", static_cast<uint8_t>(endpoint.version),
                  prefix, endpoint.path);
  VAST_INFO("setting up route {}", path); // FIXME: INFO -> debug
  router->add_handler(
    method, path,
    [&self, &endpoint, handler](auto req,
                                restinio::router::route_params_t /*params*/)
      -> restinio::request_handling_status_t {
      if (s_require_authentication) {
        auto token = req->header().try_get_field("X-VAST-Token");
        if (!token) {
          req->create_response(restinio::status_unauthorized())
            .set_body("missing `X-VAST-Token` header")
            .done();
          return restinio::request_rejected();
        }
        VAST_INFO("got token: {}", *token); // FIXME: remove msg
        if (std::find(s_tokens.begin(), s_tokens.end(), *token)
            == s_tokens.end()) {
          req->create_response(restinio::status_unauthorized())
            .set_body("invalid token")
            .done();
          return restinio::request_rejected();
        }
      }
      // TODO: Convert params to `vast::record` and validate.
      // auto as_data = convert<>(params)
      auto vast_request = vast::http_request{
        .params = vast::record{}, // FIXME
        .response
        = std::make_shared<restinio_response>(std::move(req), endpoint),
      };
      // TODO: Choose reasonable request timeout.
      self->send(handler, atom::http_request_v, endpoint.endpoint_id,
                 std::move(vast_request));
      return restinio::request_accepted();
    });
}

auto server_command(const vast::invocation& inv, caf::actor_system& system)
  -> caf::message {
  auto self = caf::scoped_actor{system};
  auto data = to<vast::data>(inv.options);
  rest::configuration config;
  convert(*data, config);
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
  // Set up router.
  auto router = std::make_unique<router_t>();
  router->non_matched_request_handler([](auto req) {
    VAST_INFO("not found: {}", req->header().path());
    return req
      ->create_response(restinio::status_not_found())
      // TODO: probably the `connection_close()` isn't needed, all it
      // seems to do is to turn `keepalive` to `off`.
      // .connection_close()
      .done();
  });
  // Set up API routes from plugins.
  for (auto const* rest_plugin : plugins::get<rest_endpoint_plugin>()) {
    auto prefix = rest_plugin->prefix();
    if (!prefix.empty() && prefix[0] != '/') {
      VAST_WARN("ignoring endpoints with invalid prefix {}", prefix);
      continue;
    }
    auto handler = rest_plugin->handler(system, node);
    for (auto const& endpoint : rest_plugin->api_endpoints()) {
      if (endpoint.path.empty() || endpoint.path[0] != '/') {
        VAST_WARN("ignoring route {} due to missing '/'", endpoint.path);
        continue;
      }
      setup_route(self, router, rest_plugin->prefix(), endpoint, handler);
    }
  }
  // Set up some non-API convenience routes.
  router->http_get(
    "/", [](auto request, auto) -> restinio::request_handling_status_t {
      return request->create_response()
        .append_header(restinio::http_field::server, "VAST")
        .append_header_date_field()
        .append_header(restinio::http_field::content_type, "text/html; "
                                                           "charset=utf-8")
        .set_body(restinio::sendfile("../plugins/rest/www/status.html"))
        .done();
    });
  // Run server.
  const server_config::server_mode server_mode
    = server_config::server_mode::debug; // FIXME
  bool require_https = true;
  bool require_clientcerts = false;
  bool require_authtoken = true;
  bool require_localhost = true;
  switch (server_mode) {
    case server_config::server_mode::debug:
      require_https = false;
      require_clientcerts = false;
      require_authtoken = false;
      require_localhost = false;
      break;
    case server_config::server_mode::upstream:
      require_https = false;
      require_clientcerts = false;
      require_authtoken = true;
      require_localhost = true;
      break;
    case server_config::server_mode::mtls:
      require_https = true;
      require_clientcerts = true;
      require_authtoken = true;
      require_localhost = false;
      break;
    case server_config::server_mode::server:
      require_https = true;
      require_clientcerts = false;
      require_authtoken = true;
      require_localhost = true;
  }
  s_require_authentication = require_authtoken;
  auto bind = config.bind_address;
  if (require_localhost)
    if (bind != "localhost" && bind != "127.0.0.1" && bind != "::1")
      return caf::make_message(caf::make_error(
        ec::invalid_argument,
        fmt::format("can only bind to localhost in {} mode", config.mode)));
  if (!require_https) {
    struct my_server_traits : public restinio::default_single_thread_traits_t {
      using request_handler_t = restinio::router::express_router_t<>;
    };
    VAST_INFO("listening on http://{}:{}", config.bind_address, config.port);
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
    // based on [1] these are mainly relevant for SSL which we don't support
    // anyways.
    // [1]: https://www.openssl.org/docs/man1.0.2/man3/SSL_CTX_set_options.html
    tls_context.set_options(asio::ssl::context::tls_server
                            | asio::ssl::context::single_dh_use);
    if (require_clientcerts) {
      tls_context.set_verify_mode(
        asio::ssl::context::verify_peer
        | asio::ssl::context::verify_fail_if_no_peer_cert);
    } else {
      tls_context.set_verify_mode(asio::ssl::context::verify_none);
    }
    tls_context.use_certificate_chain_file(config.certfile);
    tls_context.use_private_key_file(config.keyfile, asio::ssl::context::pem);
    tls_context.use_tmp_dh_file(config.dhtmpfile);
    VAST_INFO("listening on https://{}:{}", config.bind_address, config.port);
    using namespace std::literals::chrono_literals;
    restinio::run(restinio::on_this_thread<traits_t>()
                    .address(config.bind_address)
                    .request_handler(std::move(router))
                    .read_next_http_message_timelimit(10s)
                    .write_http_response_timelimit(1s)
                    .handle_request_timeout(1s)
                    .tls_context(std::move(tls_context)));
  }
  // FIXME: Kill spawned actors.
  return {};
}

} // namespace vast::plugins::rest
