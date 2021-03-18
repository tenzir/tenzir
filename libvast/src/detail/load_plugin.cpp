// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/load_plugin.hpp"

#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/process.hpp"
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
#if !VAST_ENABLE_RELOCATABLE_INSTALLATIONS
  result.insert(std::filesystem::path{VAST_LIBDIR} / "vast" / "plugins");
#endif
  // FIXME: we technically should not use "lib" relative to the parent, because
  // it may be lib64 or something else. CMAKE_INSTALL_LIBDIR is probably the
  // best choice.
  if (auto binary = objectpath(nullptr))
    result.insert(binary->parent_path().parent_path() / "lib" / "vast"
                  / "plugins");
  else
    VAST_ERROR("{} failed to get program path", __func__);
  if (const char* home = std::getenv("HOME"))
    result.insert(std::filesystem::path{home} / ".local" / "lib" / "vast"
                  / "plugins");
  if (auto dirs = caf::get_if<std::vector<std::string>>( //
        &cfg, "vast.plugin-dirs"))
    result.insert(dirs->begin(), dirs->end());
  return result;
}

} // namespace

caf::expected<std::pair<std::filesystem::path, plugin_ptr>>
load_plugin(std::filesystem::path file, caf::actor_system_config& cfg) {
  auto& plugins = plugins::get();
  auto try_load_plugin
    = [&](std::filesystem::path file) -> caf::expected<plugin_ptr> {
#if VAST_MACOS
    if (file.extension() == "")
      file += ".dylib";
#else
    if (file.extension() == "")
      file += ".so";
#endif
    if (!exists(file))
      return caf::no_error;
    auto plugin = plugin_ptr::make(file.c_str(), cfg);
    if (plugin) {
      VAST_ASSERT(*plugin);
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
  auto plugin = try_load_plugin(file);
  if (plugin)
    return std::pair{file, std::move(*plugin)};
  if (plugin.error() != caf::no_error)
    load_errors.push_back(std::move(plugin.error()));
  // Second, check if the plugin file is specified relative to the specified
  // plugin directories.
  for (const auto& dir : get_plugin_dirs(cfg))
    if (auto plugin = try_load_plugin(dir / file))
      return std::pair{dir / file, std::move(*plugin)};
    else if (plugin.error() != caf::no_error)
      load_errors.push_back(std::move(plugin.error()));
  // We didn't find the plugin, and did not encounter any errors, so the file
  // just does not exist.
  if (load_errors.empty())
    return caf::make_error(ec::invalid_configuration,
                           fmt::format("failed to find plugin {}", file));
  // We found the file, but encounterd errors trying to load it.
  return caf::make_error(ec::invalid_configuration,
                         fmt::format("failed to load plugin {}:\n - {}", file,
                                     fmt::join(load_errors, "\n - ")));
}

} // namespace vast::detail
