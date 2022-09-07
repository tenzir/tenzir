//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/convertible/data.hpp>
#include <vast/concept/convertible/to.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/data.hpp>
#include <vast/plugin.hpp>
#include <vast/type.hpp>

#include <rest/configuration.hpp>
#include <rest/server_command.hpp>
#include <rest/server_state.hpp>

namespace vast::plugins::rest {

/// The API plugin.
class plugin final : public virtual command_plugin {
  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  [[nodiscard]] caf::error initialize(data data) override {
    auto config = to<configuration>(data);
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "rest";
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
    auto token_command
      = [](const vast::invocation&, caf::actor_system&) -> caf::message {
      auto& server = server_singleton();
      auto token = server.generate();
      fmt::print("{}\n", token);
      return {};
    };
    auto spec_command
      = [](const vast::invocation&, caf::actor_system&) -> caf::message {
      // TODO: It would probably make more sense to execute this on the server?
      auto paths = record{};
      for (auto const* plugin : plugins::get<rest_endpoint_plugin>()) {
        auto spec = plugin->openapi_specification();
        VAST_ASSERT_CHEAP(caf::holds_alternative<record>(spec));
        for (auto& [key, value] : caf::get<record>(spec))
          paths.emplace(key, value);
      }
      auto openapi = record{
        {"openapi", "3.0.0"},
        {"info",
         record{
           {"description", "VAST API"},
           {"version", "0.1"},
         }},
        {"paths", std::move(paths)},
      };
      auto yaml = to_yaml(openapi);
      VAST_ASSERT_CHEAP(yaml);
      fmt::print("---\n{}\n", *yaml);
      return {};
    };
    auto factory = command::factory{};
    factory["rest server"] = rest::server_command;
    factory["rest generate-token"] = token_command;
    factory["rest specification"] = spec_command;
    return {std::move(rest_command), std::move(factory)};
  }
};

} // namespace vast::plugins::rest

VAST_REGISTER_PLUGIN(vast::plugins::rest::plugin)
