//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"

#include "vast/concept/convertible/to.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/node.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>

#include <dlfcn.h>
#include <memory>
#include <tuple>

namespace vast {

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

namespace {

detail::stable_set<std::filesystem::path>
get_plugin_dirs(const caf::actor_system_config& cfg) {
  detail::stable_set<std::filesystem::path> result;
  const auto bare_mode = caf::get_or(cfg, "vast.bare-mode", false);
  // Since we do not read configuration files that were not explicitly
  // specified when in bare-mode, it is safe to just read the option
  // `vast.plugin-dirs` even with bare-mode enabled.
  if (auto dirs = caf::get_if<std::vector<std::string>>( //
        &cfg, "vast.plugin-dirs"))
    result.insert(dirs->begin(), dirs->end());
  if (!bare_mode)
    if (auto home = detail::locked_getenv("HOME"))
      result.insert(std::filesystem::path{*home} / ".local" / "lib" / "vast"
                    / "plugins");
  result.insert(detail::install_plugindir());
  return result;
}

caf::expected<std::filesystem::path>
resolve_plugin_name(const detail::stable_set<std::filesystem::path>& plugin_dirs,
                    std::string_view name) {
  for (const auto& dir : plugin_dirs) {
    auto maybe_path = dir
                      / fmt::format("libvast-plugin-{}.{}", name,
                                    VAST_MACOS ? "dylib" : "so");
    auto ec = std::error_code{};
    if (std::filesystem::is_regular_file(maybe_path, ec))
      return maybe_path;
  }
  return caf::make_error(ec::invalid_configuration,
                         fmt::format("failed to find the {} plugin", name));
}

std::vector<std::filesystem::path> loaded_config_files_singleton = {};

} // namespace

std::vector<plugin_ptr>& get_mutable() noexcept {
  static auto plugins = std::vector<plugin_ptr>{};
  return plugins;
}

const std::vector<plugin_ptr>& get() noexcept {
  return get_mutable();
}

std::vector<std::pair<plugin_type_id_block, void (*)(caf::actor_system_config&)>>&
get_static_type_id_blocks() noexcept {
  static auto result = std::vector<
    std::pair<plugin_type_id_block, void (*)(caf::actor_system_config&)>>{};
  return result;
}

caf::expected<std::vector<std::filesystem::path>>
load(std::vector<std::string> bundled_plugins, caf::actor_system_config& cfg) {
  auto loaded_plugin_paths = std::vector<std::filesystem::path>{};
  // Step 1: Get the necessary options.
  auto paths_or_names
    = caf::get_or(cfg, "vast.plugins", std::vector<std::string>{});
  if (paths_or_names.empty() && bundled_plugins.empty())
    return loaded_plugin_paths;
  const auto plugin_dirs = get_plugin_dirs(cfg);
  // Step 2: Try to resolve the reserved identifier 'all'. The list may only
  // contain plugin names, plugin paths, and the resevved identifier 'bundled'
  // afterwards.
  if (const auto all
      = std::remove(paths_or_names.begin(), paths_or_names.end(), "all");
      all != paths_or_names.end()) {
    paths_or_names.erase(all, paths_or_names.end());
    for (const auto& dir : plugin_dirs) {
      auto ec = std::error_code{};
      for (const auto& file : std::filesystem::directory_iterator{dir, ec}) {
        if (ec || !file.is_regular_file())
          break;
        if (file.path().filename().string().starts_with("libvast-plugin-"))
          paths_or_names.push_back(file.path());
      }
    }
    // 'all' implies 'bundled'.
    paths_or_names.emplace_back("bundled");
  }
  // Step 3: Try to resolve the reserved identifier 'bundled' into a list of
  // plugin names. The list may only contain plugin names and plugin paths
  // afterwards.
  if (const auto bundled
      = std::remove(paths_or_names.begin(), paths_or_names.end(), "bundled");
      bundled != paths_or_names.end()) {
    paths_or_names.erase(bundled, paths_or_names.end());
    std::copy(bundled_plugins.begin(), bundled_plugins.end(),
              std::back_inserter(paths_or_names));
  }
  // Step 4: Disable static plugins that were not enabled, and remove the names
  // of static plugins from the list of enabled plugins.
  auto check_and_remove_disabled_static_plugin = [&](auto& plugin) -> bool {
    switch (plugin.type()) {
      case plugin_ptr::type::dynamic:
        die("dynamic plugins must not be loaded at this point");
      case plugin_ptr::type::static_: {
        auto has_same_name = [&](const auto& name) {
          return plugin->name() == name;
        };
        auto it = std::remove_if(paths_or_names.begin(), paths_or_names.end(),
                                 has_same_name);
        const auto result = it == paths_or_names.end();
        paths_or_names.erase(it, paths_or_names.end());
        return result;
      }
      case plugin_ptr::type::native:
        return false;
    }
    die("unreachable");
  };
  get_mutable().erase(std::remove_if(get_mutable().begin(), get_mutable().end(),
                                     check_and_remove_disabled_static_plugin),
                      get_mutable().end());
  // Step 5: Try to resolve plugin names to plugin paths. After this step, the
  // list only contains plugin paths.
  for (auto& path_or_name : paths_or_names) {
    // Ignore paths.
    if (auto maybe_path = std::filesystem::path{path_or_name};
        maybe_path.is_absolute())
      continue;
    // At this point, we only have names—that we need to resolve to
    // `{dir}/libvast-plugin-{name}.{ext}`. We take the first file that
    // exists.
    if (auto path = resolve_plugin_name(plugin_dirs, path_or_name))
      path_or_name = path->string();
    else
      return std::move(path.error());
  }
  // Step 6: Deduplicate plugin paths.
  // TODO: Consider moving steps 1-5 into a separate `resolve` function, and
  // splitting step 3 into separate steps for modifying the list of bundled
  // plugins and unloading unwanted static plugins.
  auto paths = detail::stable_set<std::string>{};
  paths.insert(std::make_move_iterator(paths_or_names.begin()),
               std::make_move_iterator(paths_or_names.end()));
  // Step 7: Load plugins.
  for (auto path : std::move(paths)) {
    if (auto plugin = plugin_ptr::make_dynamic(path.c_str(), cfg)) {
      // Check for name clashes.
      auto has_same_name = [&](const auto& other) {
        return !std::strcmp((*plugin)->name(), other->name());
      };
      if (std::any_of(get().begin(), get().end(), has_same_name))
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("failed to load the {} plugin "
                                           "because another plugin already "
                                           "uses the name {}",
                                           path, (*plugin)->name()));
      get_mutable().push_back(std::move(*plugin));
      loaded_plugin_paths.emplace_back(std::move(path));
    } else {
      return std::move(plugin.error());
    }
  }
  // Step 8: Sort loaded plugins by name.
  std::sort(get_mutable().begin(), get_mutable().end(),
            [](const auto& lhs, const auto& rhs) {
              return std::strcmp(lhs->name(), rhs->name()) < 0;
            });
  return loaded_plugin_paths;
}

/// Initialize loaded plugins.
caf::error initialize(caf::actor_system_config& cfg) {
  for (auto& plugin : get_mutable()) {
    auto merged_config = record{};
    // First, try to read the configuration from the merged VAST configuration.
    if (auto opts = caf::get_if<caf::settings>(
          &cfg, fmt::format("plugins.{}", plugin->name()))) {
      if (auto opts_data = to<record>(*opts))
        merged_config = std::move(*opts_data);
      else
        VAST_DEBUG("unable to read plugin options from VAST configuration at "
                   "plugins.{}: {}",
                   opts_data.error(), plugin->name());
    }
    // Second, try to read the configuration from the plugin-specific
    // configuration files at <config-dir>/plugin/<plugin-name>.yaml.
    for (auto&& config_dir : system::config_dirs(cfg)) {
      const auto yaml_path
        = config_dir / "plugin" / fmt::format("{}.yaml", plugin->name());
      const auto yml_path
        = config_dir / "plugin" / fmt::format("{}.yml", plugin->name());
      auto err = std::error_code{};
      const auto yaml_path_exists = std::filesystem::exists(yaml_path, err);
      err.clear();
      const auto yml_path_exists = std::filesystem::exists(yml_path, err);
      if (!yaml_path_exists && !yml_path_exists)
        continue;
      if (yaml_path_exists && yml_path_exists)
        return caf::make_error(
          ec::invalid_configuration,
          fmt::format("detected configuration files for the {} plugin at "
                      "conflicting paths {} and {}",
                      plugin->name(), yaml_path, yml_path));
      const auto& path = yaml_path_exists ? yaml_path : yml_path;
      if (auto opts = load_yaml(path)) {
        // Skip empty config files.
        if (caf::holds_alternative<caf::none_t>(*opts))
          continue;
        if (auto opts_data = caf::get_if<record>(&*opts)) {
          merge(*opts_data, merged_config, policy::merge_lists::yes);
          VAST_INFO("loaded plugin configuration file: {}", path);
          loaded_config_files_singleton.push_back(path);
        } else {
          return caf::make_error(ec::invalid_configuration,
                                 fmt::format("detected invalid plugin "
                                             "configuration file for the {} "
                                             "plugin at {}",
                                             plugin->name(), path));
        }
      } else {
        return std::move(opts.error());
      }
    }
    // Third, initialize the plugin with the merged configuration.
    VAST_VERBOSE("initializing the {} plugin with options: {}", plugin->name(),
                 merged_config);
    if (auto err = plugin->initialize(std::move(merged_config)))
      return caf::make_error(
        ec::unspecified, fmt::format("failed to initialize the {} plugin: {} ",
                                     plugin->name(), err));
  }
  return caf::none;
}

const std::vector<std::filesystem::path>& loaded_config_files() {
  return loaded_config_files_singleton;
}

} // namespace plugins

// -- analyzer plugin ---------------------------------------------------------

system::analyzer_plugin_actor analyzer_plugin::analyzer(
  system::node_actor::stateful_pointer<system::node_state> node) const {
  if (auto handle = weak_handle_.lock())
    return caf::actor_cast<system::analyzer_plugin_actor>(handle);
  if (spawned_once_ || !node)
    return {};
  auto handle = make_analyzer(node);
  auto [importer] = node->state.registry.find<system::importer_actor>();
  VAST_ASSERT(importer);
  node
    ->request(importer, caf::infinite,
              static_cast<system::stream_sink_actor<table_slice>>(handle))
    .then([](const caf::outbound_stream_slot<table_slice>&) {},
          [&](const caf::error& error) {
            VAST_ERROR("failed to connect analyzer {} to the importer: {}",
                       name(), error);
          });
  weak_handle_ = caf::actor_cast<caf::weak_actor_ptr>(handle);
  spawned_once_ = true;
  return handle;
}

system::component_plugin_actor analyzer_plugin::make_component(
  system::node_actor::stateful_pointer<system::node_state> node) const {
  return analyzer(node);
}

// -- plugin_ptr ---------------------------------------------------------------

caf::expected<plugin_ptr>
plugin_ptr::make_dynamic(const char* filename,
                         caf::actor_system_config& cfg) noexcept {
  auto* library = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY);
  if (!library)
    return caf::make_error(ec::system_error, "failed to load plugin", filename,
                           dlerror());
  auto libvast_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "vast_libvast_version"));
  if (!libvast_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_libvast_version in",
                           filename, dlerror());
  if (strcmp(libvast_version(), version::version) != 0)
    return caf::make_error(ec::version_error, "libvast version mismatch in",
                           filename, libvast_version(), version::version);
  auto libvast_build_tree_hash = reinterpret_cast<const char* (*)()>(
    dlsym(library, "vast_libvast_build_tree_hash"));
  if (!libvast_build_tree_hash)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "vast_libvast_build_tree_hash in",
                           filename, dlerror());
  if (strcmp(libvast_build_tree_hash(), version::build_tree_hash) != 0)
    return caf::make_error(ec::version_error,
                           "libvast build tree hash mismatch in", filename,
                           libvast_build_tree_hash(), version::build_tree_hash);
  auto plugin_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "vast_plugin_version"));
  if (!plugin_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_version in",
                           filename, dlerror());
  auto plugin_create = reinterpret_cast<::vast::plugin* (*)()>(
    dlsym(library, "vast_plugin_create"));
  if (!plugin_create)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_create in",
                           filename, dlerror());
  auto plugin_destroy = reinterpret_cast<void (*)(::vast::plugin*)>(
    dlsym(library, "vast_plugin_destroy"));
  if (!plugin_destroy)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol vast_plugin_destroy in",
                           filename, dlerror());
  auto plugin_type_id_block
    = reinterpret_cast<::vast::plugin_type_id_block (*)()>(
      dlsym(library, "vast_plugin_type_id_block"));
  if (plugin_type_id_block) {
    auto plugin_register_type_id_block
      = reinterpret_cast<void (*)(::caf::actor_system_config&)>(
        dlsym(library, "vast_plugin_register_type_id_block"));
    if (!plugin_register_type_id_block)
      return caf::make_error(ec::system_error,
                             "failed to resolve symbol "
                             "vast_plugin_register_type_id_block in",
                             filename, dlerror());
    // If the plugin requested to add additional type ID blocks, check if the
    // ranges overlap. Since this is static for the whole process, we just store
    // the already registed ID blocks from plugins in a static variable.
    static auto old_blocks = std::vector<::vast::plugin_type_id_block>{
      {caf::id_block::vast_types::begin, caf::id_block::vast_actors::end}};
    // Static plugins are built as part of the vast binary rather then libvast,
    // so there will be runtime errors when there is a type ID clash between
    // static and dynamic plugins. We register the ID blocks of all static
    // plugins exactly once to always prefer them over dynamic plugins.
    static auto flag = std::once_flag{};
    std::call_once(flag, [&] {
      for (const auto& [block, _] : plugins::get_static_type_id_blocks())
        old_blocks.push_back(block);
    });
    auto new_block = plugin_type_id_block();
    for (const auto& old_block : old_blocks)
      if (new_block.begin < old_block.end && old_block.begin < new_block.end)
        return caf::make_error(ec::system_error,
                               "encountered type ID block clash in", filename);
    plugin_register_type_id_block(cfg);
    old_blocks.push_back(new_block);
  }
  return plugin_ptr{library, plugin_create(), plugin_destroy, plugin_version(),
                    type::dynamic};
}

plugin_ptr plugin_ptr::make_static(plugin* instance, void (*deleter)(plugin*),
                                   const char* version) noexcept {
  return plugin_ptr{nullptr, instance, deleter, version, type::static_};
}

plugin_ptr plugin_ptr::make_native(plugin* instance, void (*deleter)(plugin*),
                                   const char* version) noexcept {
  return plugin_ptr{nullptr, instance, deleter, version, type::native};
}

plugin_ptr::plugin_ptr() noexcept = default;

plugin_ptr::~plugin_ptr() noexcept {
  if (instance_) {
    VAST_ASSERT(deleter_);
    deleter_(instance_);
    instance_ = {};
    deleter_ = {};
  }
  if (library_) {
    dlclose(library_);
    library_ = {};
  }
  version_ = {};
  type_ = {};
}

plugin_ptr::plugin_ptr(plugin_ptr&& other) noexcept
  : library_{std::exchange(other.library_, {})},
    instance_{std::exchange(other.instance_, {})},
    deleter_{std::exchange(other.deleter_, {})},
    version_{std::exchange(other.version_, {})},
    type_{std::exchange(other.type_, {})} {
  // nop
}

plugin_ptr& plugin_ptr::operator=(plugin_ptr&& rhs) noexcept {
  library_ = std::exchange(rhs.library_, {});
  instance_ = std::exchange(rhs.instance_, {});
  deleter_ = std::exchange(rhs.deleter_, {});
  version_ = std::exchange(rhs.version_, {});
  type_ = std::exchange(rhs.type_, {});
  return *this;
}

plugin_ptr::operator bool() const noexcept {
  return static_cast<bool>(instance_);
}

const plugin* plugin_ptr::operator->() const noexcept {
  return instance_;
}

plugin* plugin_ptr::operator->() noexcept {
  return instance_;
}

const plugin& plugin_ptr::operator*() const noexcept {
  return *instance_;
}

plugin& plugin_ptr::operator&() noexcept {
  return *instance_;
}

plugin_ptr::plugin_ptr(void* library, plugin* instance,
                       void (*deleter)(plugin*), const char* version,
                       enum type type) noexcept
  : library_{library},
    instance_{instance},
    deleter_{deleter},
    version_{version},
    type_{type} {
  // nop
}

const char* plugin_ptr::version() const noexcept {
  return version_;
}

enum plugin_ptr::type plugin_ptr::type() const noexcept {
  return type_;
}

} // namespace vast
