//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "rest/authenticator.hpp"
#include "rest/configuration.hpp"
#include "rest/generate_token_command.hpp"
#include "rest/server_command.hpp"
#include "rest/specification_command.hpp"

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/data.hpp>
#include <vast/plugin.hpp>
#include <vast/system/node.hpp>
#include <vast/system/node_control.hpp>
#include <vast/system/spawn_or_connect_to_node.hpp>
#include <vast/type.hpp>

#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::plugins::rest {

/// The API plugin.
class plugin final : public virtual command_plugin,
                     public virtual component_plugin {
  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  [[nodiscard]] caf::error initialize(data data) override {
    // FIXME
    auto config = to<configuration>(data);
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "rest";
  }

  system::component_plugin_actor
  make_component(system::node_actor::stateful_pointer<system::node_state> node)
    const override {
    auto [filesystem] = node->state.registry.find<system::filesystem_actor>();
    return node->spawn(authenticator, std::move(filesystem));
  }

  /// Creates additional commands.
  /// @note VAST calls this function before initializing the plugin, which
  /// means that this function cannot depend on any plugin state. The logger
  /// is unavailable when this function is called.
  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto rest_command = std::make_unique<command>(
      "rest", "rest api", command::opts("?plugins.rest"));
    rest_command->add_subcommand(
      "server", "start a web server",
      command::opts("?rest")
        .add<bool>("help,h?", "prints the help text")
        .add<std::string>("mode", "Server mode. One of "
                                  "dev,server,upstream,mtls.")
        .add<std::string>("certificate-path", "path to TLS cert")
        .add<std::string>("key-path", "path to TLS private key")
        .add<std::string>("bind", "listen address of server")
        .add<uint16_t>("port", "listen port"));
    rest_command->add_subcommand("generate-token", "generate auth token",
                                 command::opts("?rest.token"));
    rest_command->add_subcommand("specification", "print openAPI spec",
                                 command::opts("?rest.spec"));
    auto factory = command::factory{};
    factory["rest server"] = rest::server_command;
    factory["rest generate-token"] = rest::generate_token_command;
    factory["rest specification"] = rest::specification_command;
    return {std::move(rest_command), std::move(factory)};
  }
};

} // namespace vast::plugins::rest

VAST_REGISTER_PLUGIN(vast::plugins::rest::plugin)
