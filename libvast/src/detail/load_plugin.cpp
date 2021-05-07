//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/load_plugin.hpp"

#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"

#include <caf/actor_system_config.hpp>

#include <filesystem>

namespace vast::detail {

namespace {

stable_set<std::filesystem::path>
get_plugin_dirs(const caf::actor_system_config& cfg) {
  stable_set<std::filesystem::path> result;
  const auto bare_mode = caf::get_or(cfg, "vast.bare-mode", false);
  if (auto vast_plugin_directories = locked_getenv("VAST_PLUGIN_DIRS"))
    for (auto&& path : detail::split(*vast_plugin_directories, ":"))
      result.insert({path});
  if (!bare_mode) {
    result.insert(install_plugindir());
    if (auto home = locked_getenv("HOME"))
      result.insert(std::filesystem::path{*home} / ".local" / "lib" / "vast"
                    / "plugins");
    if (auto dirs = caf::get_if<std::vector<std::string>>( //
          &cfg, "vast.plugin-dirs"))
      result.insert(dirs->begin(), dirs->end());
  }
  return result;
}

} // namespace

caf::expected<std::pair<std::filesystem::path, plugin_ptr>>
load_plugin(const std::filesystem::path& path_or_name,
            caf::actor_system_config& cfg) {
  auto& plugins = plugins::get();
  auto try_load_plugin = [&](const std::filesystem::path& root,
                             const std::filesystem::path& path_or_name)
    -> caf::expected<plugin_ptr> {
#if VAST_MACOS
    static constexpr auto ext = ".dylib";
#else
    static constexpr auto ext = ".so";
#endif
    const bool specified_by_name
      = !path_or_name.has_parent_path() && path_or_name.extension().empty();
    // A root must be configured if the plugin is specified by name rather than
    // path. This check ensures we do not silently pick up plugins in the
    // current working directory.
    if (specified_by_name && root.empty())
      return caf::no_error;
    auto file = specified_by_name
                  ? root
                      / std::filesystem::path{"libvast-plugin-"
                                              + path_or_name.string() + ext}
                  : path_or_name;
    if (!file.is_absolute() && !root.empty())
      file = root / file;
    if (!exists(file))
      return caf::no_error;
    auto plugin = plugin_ptr::make_dynamic(file.c_str(), cfg);
    if (plugin) {
      VAST_ASSERT(*plugin);
      if (specified_by_name)
        if ((*plugin)->name() != path_or_name.string())
          return caf::make_error( //
            ec::invalid_configuration,
            fmt::format("failed to load plugin {} because its name {} does not "
                        "match the expected name {}",
                        file, (*plugin)->name(), path_or_name));
      auto has_same_name = [name = (*plugin)->name()](const auto& other) {
        return !std::strcmp(name, other->name());
      };
      if (std::none_of(plugins.begin(), plugins.end(), has_same_name))
        return plugin;
      return caf::make_error(ec::invalid_configuration,
                             fmt::format("failed to load plugin {} because "
                                         "another plugin already uses the "
                                         "name {}",
                                         file, (*plugin)->name()));
    }
    return std::move(plugin.error());
  };
  auto load_errors = std::vector<caf::error>{};
  // First, check if the plugin file is specified as an absolute path.
  auto plugin = try_load_plugin({}, path_or_name);
  if (plugin)
    return std::pair{path_or_name, std::move(*plugin)};
  if (plugin.error() != caf::no_error)
    load_errors.push_back(std::move(plugin.error()));
  // Second, check if the plugin file is specified relative to the specified
  // plugin directories.
  for (const auto& dir : get_plugin_dirs(cfg)) {
    if (auto plugin = try_load_plugin(dir, path_or_name))
      return std::pair{dir / path_or_name, std::move(*plugin)};
    else if (plugin.error() != caf::no_error)
      load_errors.push_back(std::move(plugin.error()));
  }
  // We didn't find the plugin, and did not encounter any errors, so the file
  // just does not exist.
  if (load_errors.empty())
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("failed to find plugin {}",
                                       path_or_name));
  // We found the file, but encounterd errors trying to load it.
  return caf::make_error(ec::invalid_configuration,
                         fmt::format("failed to load plugin {}:\n - {}",
                                     path_or_name,
                                     fmt::join(load_errors, "\n - ")));
}

} // namespace vast::detail
