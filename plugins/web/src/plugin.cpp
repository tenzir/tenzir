//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "web/authenticator.hpp"
#include "web/configuration.hpp"
#include "web/generate_token_command.hpp"
#include "web/server_command.hpp"
#include "web/specification_command.hpp"

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/data.hpp>
#include <vast/node.hpp>
#include <vast/node_control.hpp>
#include <vast/plugin.hpp>
#include <vast/spawn_or_connect_to_node.hpp>
#include <vast/type.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::web {

/// The API plugin.
class plugin final : public virtual command_plugin,
                     public virtual component_plugin {
  [[nodiscard]] caf::error
  initialize([[maybe_unused]] const record& plugin_config,
             [[maybe_unused]] const record& global_config) override {
    // We don't need to do anything here since the plugin config currently
    // only applies to the server command, which gets its own settings.
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] std::string name() const override {
    return "web";
  }

  component_plugin_actor
  make_component(node_actor::stateful_pointer<node_state> node) const override {
    auto [filesystem] = node->state.registry.find<filesystem_actor>();
    return node->spawn(authenticator, std::move(filesystem));
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto rest_command = std::make_unique<command>(
      "web", "http server", command::opts("?plugins.web"));
    rest_command->add_subcommand(
      "server", "start a web server",
      command::opts("?plugins.web")
        .add<bool>("help,h?", "prints the help text")
        .add<std::string>("mode", "Server mode. One of "
                                  "dev,server,upstream,mtls.")
        .add<std::string>("certfile", "path to TLS server certificate")
        .add<std::string>("keyfile", "path to TLS private key")
        .add<std::string>("allowed-origin", "allowed origin for CORS requests; "
                                            "defaults to '*' in dev mode.")
        .add<std::string>("root", "document root of the server")
        .add<std::string>("bind", "listen address of server")
        .add<int64_t>("port", "listen port"));
    rest_command->add_subcommand("generate-token", "generate auth token",
                                 command::opts("?plugins.web.token"));
    rest_command->add_subcommand("openapi", "print openAPI spec",
                                 command::opts("?plugins.web.spec"));
    auto factory = command::factory{};
    factory["web server"] = web::server_command;
    factory["web generate-token"] = web::generate_token_command;
    factory["web openapi"] = web::specification_command;
    return {std::move(rest_command), std::move(factory)};
  }
};

} // namespace vast::plugins::web

VAST_REGISTER_PLUGIN(vast::plugins::web::plugin)
VAST_REGISTER_PLUGIN_TYPE_ID_BLOCK(vast_web_plugin_types,
                                   vast_web_plugin_actors)
