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

#include <rest/server_command.hpp>

namespace vast::plugins::rest {

struct configuration {
  static const record_type& layout() noexcept {
    static auto result = vast::record_type{
      {"bind", vast::string_type{}},
    };
    return result;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, configuration& x) {
    return f(caf::meta::type_name("vast.plugins.rest.configuration"),
             x.bind_address);
  }

  std::string bind_address;
};

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
    auto rest_command
      = std::make_unique<command>("server", "start a web server",
                                  "starts a server for the REST API of vast",
                                  command::opts());
    auto factory = command::factory{
      {"server", &plugins::rest::server_command},
    };
    return {std::move(rest_command), std::move(factory)};
  }
};

} // namespace vast::plugins::rest

VAST_REGISTER_PLUGIN(vast::plugins::rest::plugin)
