//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"

#include <caf/actor_system_config.hpp>

#include <filesystem>
#include <string>
#include <vector>

namespace tenzir {

struct config_file {
  std::filesystem::path path;
  std::optional<std::string> plugin;

  auto operator==(const config_file& rhs) const -> bool = default;
  auto operator<=>(const config_file& rhs) const -> std::strong_ordering
    = default;
};

/// @returns The config dirs of the application.
/// @param cfg The actor system config to introspect.
auto config_dirs(const caf::actor_system_config& cfg)
  -> std::vector<std::filesystem::path>;

auto config_dirs(const record& cfg) -> std::vector<std::filesystem::path>;

/// @returns The loaded config files of the application.
/// @note This function is not threadsafe.
auto loaded_config_files() -> const std::vector<config_file>&;

/// @returns The duration value of the given option.
auto get_or_duration(const caf::settings& options, std::string_view key,
                     duration fallback) -> caf::expected<duration>;

/// Bundles all configuration parameters of a Tenzir system.
class configuration : public caf::actor_system_config {
public:
  // -- constructors, destructors, and assignment operators --------------------

  configuration();

  // -- modifiers --------------------------------------------------------------

  auto parse(int argc, char** argv) -> caf::error;

  // -- configuration options --------------------------------------------------

  /// The program command line, without --caf. arguments.
  std::vector<std::string> command_line = {};

  /// The configuration files to load.
  std::vector<config_file> config_files = {};

private:
  auto embed_config(const caf::settings& settings) -> caf::error;
};

} // namespace tenzir
