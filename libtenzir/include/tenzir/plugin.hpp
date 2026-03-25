//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/plugin/register.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/error.hpp>

#include <cctype>
#include <filesystem>
#include <string_view>

namespace tenzir::plugins {

/// Load plugins specified in the configuration.
/// @param bundled_plugins The names of the bundled plugins.
/// @param cfg The actor system configuration of Tenzir for registering
/// additional type ID blocks.
/// @returns A list of paths to the loaded plugins, or an error detailing what
/// went wrong.
/// @note Invoke exactly once before \ref get() may be used.
auto load(const std::vector<std::string>& bundled_plugins,
          caf::actor_system_config& cfg)
  -> caf::expected<std::vector<std::filesystem::path>>;

/// Initialize loaded plugins.
auto initialize(caf::actor_system_config& cfg) -> caf::error;

/// @returns The loaded plugin-specific config files.
/// @note This function is not threadsafe.
auto loaded_config_files() -> const std::vector<std::filesystem::path>&;

} // namespace tenzir::plugins

// -- component plugin --------------------------------------------------------
// Extracted to plugin/component.hpp
#include "tenzir/plugin/component.hpp"

// -- command plugin -----------------------------------------------------------
#include "tenzir/plugin/command.hpp"

// -- operator plugin ----------------------------------------------------------
#include "tenzir/plugin/operator.hpp"

// -- loader plugin -----------------------------------------------------------
#include "tenzir/plugin/loader.hpp"

// -- parser plugin -----------------------------------------------------------
#include "tenzir/plugin/parser.hpp"

// -- printer plugin ----------------------------------------------------------
#include "tenzir/plugin/printer.hpp"

// -- saver plugin ------------------------------------------------------------
#include "tenzir/plugin/saver.hpp"

// -- rest endpoint plugin -----------------------------------------------------
#include "tenzir/plugin/rest_endpoint.hpp"

// -- store plugin ------------------------------------------------------------
#include "tenzir/plugin/store.hpp"

// -- metrics plugin ----------------------------------------------------------
#include "tenzir/plugin/metrics.hpp"

// -- aspect plugin ------------------------------------------------------------
#include "tenzir/plugin/aspect.hpp"

// -- template function definitions -------------------------------------------

namespace tenzir::plugins {

inline auto find_operator(std::string_view name) noexcept
  -> const operator_parser_plugin* {
  for (const auto* plugin : get<operator_parser_plugin>()) {
    const auto current_name = plugin->operator_name();
    const auto match
      = std::equal(current_name.begin(), current_name.end(), name.begin(),
                   name.end(), [](const char lhs, const char rhs) {
                     return std::tolower(static_cast<unsigned char>(lhs))
                            == std::tolower(static_cast<unsigned char>(rhs));
                   });
    if (match) {
      return plugin;
    }
  }
  return nullptr;
}

} // namespace tenzir::plugins
