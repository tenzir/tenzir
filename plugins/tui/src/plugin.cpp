//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tui/tui_command.hpp"

#include <vast/plugin.hpp>

namespace vast::plugins::tui {

/// The TUI plugin.
class plugin final : public virtual command_plugin {
  [[nodiscard]] caf::error initialize(data) override {
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] const char* name() const override {
    return "tui";
  }

  [[nodiscard]] std::pair<std::unique_ptr<command>, command::factory>
  make_command() const override {
    auto tui_command = std::make_unique<command>(
      "tui", "terminal user interface", command::opts("?plugins.tui"));
    auto factory = command::factory{};
    factory["tui"] = tui::tui_command;
    return {std::move(tui_command), std::move(factory)};
  }
};

} // namespace vast::plugins::tui

VAST_REGISTER_PLUGIN(vast::plugins::tui::plugin)
