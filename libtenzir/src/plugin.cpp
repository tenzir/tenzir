//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugin.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/concept/convertible/to.hpp"
#include "tenzir/config.hpp"
#include "tenzir/configuration.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/installdirs.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/detail/stable_set.hpp"
#include "tenzir/die.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/store.hpp"
#include "tenzir/uuid.hpp"

#include <arrow/api.h>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>

#include <algorithm>
#include <dlfcn.h>
#include <memory>

#ifndef __has_feature
#  define __has_feature(x) 0
#endif

#if __has_feature(address_sanitizer) or defined(__SANITIZE_ADDRESS__)
#  if __has_include(<sanitizer/lsan_interface.h>)
#    define TENZIR_HAS_LEAK_SANITIZER
#  endif
#endif

#ifdef TENZIR_HAS_LEAK_SANITIZER
#  include <sanitizer/lsan_interface.h>
#  define TENZIR_DISABLE_LEAK_SANITIZER()                                      \
    do {                                                                       \
      __lsan_disable();                                                        \
    } while (false)
#  define TENZIR_ENABLE_LEAK_SANITIZER()                                       \
    do {                                                                       \
      __lsan_enable();                                                         \
    } while (false)
#else
#  define TENZIR_DISABLE_LEAK_SANITIZER()
#  define TENZIR_ENABLE_LEAK_SANITIZER()
#endif

namespace tenzir {

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

namespace {

detail::stable_set<std::filesystem::path>
get_plugin_dirs(const caf::actor_system_config& cfg) {
  detail::stable_set<std::filesystem::path> result;
  const auto bare_mode = caf::get_or(cfg, "tenzir.bare-mode", false);
  // Since we do not read configuration files that were not explicitly
  // specified when in bare-mode, it is safe to just read the option
  // `tenzir.plugin-dirs` even with bare-mode enabled.
  if (auto dirs = detail::unpack_config_list_to_vector<std::string>( //
        cfg, "tenzir.plugin-dirs")) {
    result.insert(dirs->begin(), dirs->end());
  } else {
    TENZIR_WARN("failed to to extract plugin dirs: {}", dirs.error());
  }
  if (!bare_mode) {
    if (auto home = detail::getenv("HOME")) {
      result.insert(std::filesystem::path{*home} / ".local" / "lib" / "tenzir"
                    / "plugins");
    }
  }
  result.insert(detail::install_plugindir());
  return result;
}

caf::expected<std::filesystem::path>
resolve_plugin_name(const detail::stable_set<std::filesystem::path>& plugin_dirs,
                    std::string_view name) {
  auto plugin_file_name = fmt::format("libtenzir-plugin-{}.{}", name,
                                      TENZIR_MACOS ? "dylib" : "so");
  for (const auto& dir : plugin_dirs) {
    auto maybe_path = dir / plugin_file_name;
    auto ec = std::error_code{};
    if (std::filesystem::is_regular_file(maybe_path, ec)) {
      return maybe_path;
    }
  }
  return caf::make_error(
    ec::invalid_configuration,
    fmt::format("failed to find the {} plugin as {} in [{}]", name,
                plugin_file_name, fmt::join(plugin_dirs, ",  ")));
}

std::vector<std::filesystem::path> loaded_config_files_singleton = {};

/// Remove builtins the given list of plugins.
std::vector<std::string>
remove_builtins(std::vector<std::string> paths_or_names) {
  std::erase_if(paths_or_names, [](const auto& path_or_name) {
    return std::any_of(plugins::get().begin(), plugins::get().end(),
                       [&](const auto& plugin) {
                         return plugin->name() == path_or_name
                                && plugin.type() == plugin_ptr::type::builtin;
                       });
  });
  return paths_or_names;
}

/// Expand the 'bundled' and 'all' keywords for the given list of plugins.
std::vector<std::string> expand_special_identifiers(
  std::vector<std::string> paths_or_names,
  const std::vector<std::string>& bundled_plugins,
  const detail::stable_set<std::filesystem::path>& plugin_dirs) {
  // Try to resolve the reserved identifier 'all'. The list may only contain
  // plugin names, plugin paths, and the reserved identifier 'bundled'
  // afterwards.
  if (const auto all
      = std::remove(paths_or_names.begin(), paths_or_names.end(), "all");
      all != paths_or_names.end()) {
    paths_or_names.erase(all, paths_or_names.end());
    for (const auto& dir : plugin_dirs) {
      auto ec = std::error_code{};
      for (const auto& file : std::filesystem::directory_iterator{dir, ec}) {
        if (ec || !file.is_regular_file()) {
          break;
        }
        if (file.path().filename().string().starts_with("libtenzir-plugin-")) {
          paths_or_names.push_back(file.path());
        }
      }
    }
    // 'all' implies 'bundled'.
    paths_or_names.emplace_back("bundled");
  }
  // Try to resolve the reserved identifier 'bundled' into a list of plugin
  // names. The list may only contain plugin names and plugin paths afterwards.
  if (const auto bundled
      = std::remove(paths_or_names.begin(), paths_or_names.end(), "bundled");
      bundled != paths_or_names.end()) {
    paths_or_names.erase(bundled, paths_or_names.end());
    std::copy(bundled_plugins.begin(), bundled_plugins.end(),
              std::back_inserter(paths_or_names));
  }
  return paths_or_names;
}

/// Unload disabled static plugins, i.e., static plugins not explicitly enabled.
std::vector<std::string>
unload_disabled_static_plugins(std::vector<std::string> paths_or_names) {
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
      case plugin_ptr::type::builtin:
        return false;
    }
    TENZIR_UNREACHABLE();
  };
  get_mutable().erase(std::remove_if(get_mutable().begin(), get_mutable().end(),
                                     check_and_remove_disabled_static_plugin),
                      get_mutable().end());
  return paths_or_names;
}

/// Resolve plugin names to a sorted set of paths.
caf::expected<detail::stable_set<std::string>> resolve_plugin_names(
  std::vector<std::string> paths_or_names,
  const detail::stable_set<std::filesystem::path>& plugin_dirs) {
  for (auto& path_or_name : paths_or_names) {
    // Ignore paths.
    if (auto maybe_path = std::filesystem::path{path_or_name};
        maybe_path.is_absolute()) {
      continue;
    }
    // At this point, we only have names—that we need to resolve to
    // `{dir}/libtenzir-plugin-{name}.{ext}`. We take the first file that
    // exists.
    if (auto path = resolve_plugin_name(plugin_dirs, path_or_name)) {
      path_or_name = path->string();
    } else {
      return std::move(path.error());
    }
  }
  // Deduplicate plugins.
  // We dedup based on the filename instead of the full path, this is useful for
  // running Tenzir with a modified plugin when the unmodified plugin is also
  // bundled with the installation. If we were to dedup on the full path this
  // situation would not be caught and the process would crash because of
  // duplicate symbols.
  auto path_map = detail::stable_map<std::string, std::string>{};
  for (auto& path : paths_or_names) {
    path_map.insert({std::filesystem::path{path}.filename(), std::move(path)});
  }
  auto paths = detail::stable_set<std::string>{};
  for (auto& [_, path] : path_map) {
    paths.insert(std::move(path));
  }
  return paths;
}

} // namespace

std::vector<plugin_ptr>& get_mutable() noexcept {
  static auto plugins = std::vector<plugin_ptr>{};
  return plugins;
}

const std::vector<plugin_ptr>& get() noexcept {
  return get_mutable();
}

std::vector<std::pair<plugin_type_id_block, void (*)()>>&
get_static_type_id_blocks() noexcept {
  static auto result
    = std::vector<std::pair<plugin_type_id_block, void (*)()>>{};
  return result;
}

caf::expected<std::vector<std::filesystem::path>>
load(const std::vector<std::string>& bundled_plugins,
     caf::actor_system_config& cfg) {
  auto loaded_plugin_paths = std::vector<std::filesystem::path>{};
  // Get the necessary options.
  auto paths_or_names
    = caf::get_or(cfg, "tenzir.plugins", std::vector<std::string>{"all"});
  if (paths_or_names.empty() && bundled_plugins.empty()) {
    return loaded_plugin_paths;
  }
  const auto plugin_dirs = get_plugin_dirs(cfg);
  // Resolve the 'bundled' and 'all' identifiers.
  paths_or_names = expand_special_identifiers(std::move(paths_or_names),
                                              bundled_plugins, plugin_dirs);
  // Silently ignore builtins if they're in the list of plugins.
  paths_or_names = remove_builtins(std::move(paths_or_names));
  // Disable static plugins that were not enabled, and remove the names of
  // static plugins from the list of enabled plugins.
  paths_or_names = unload_disabled_static_plugins(std::move(paths_or_names));
  // Try to resolve plugin names to plugin paths. After this step, the list only
  // contains deduplicated plugin paths.
  auto paths = resolve_plugin_names(std::move(paths_or_names), plugin_dirs);
  if (!paths) {
    return std::move(paths.error());
  }
  // Load plugins.
  for (auto path : std::move(*paths)) {
    if (auto plugin = plugin_ptr::make_dynamic(path.c_str(), cfg)) {
      // Check for name clashes.
      if (find((*plugin)->name())) {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("failed to load the {} plugin "
                                           "because another plugin already "
                                           "uses the name {}",
                                           path, (*plugin)->name()));
      }
      const auto it = std::ranges::upper_bound(plugins::get_mutable(), *plugin);
      plugins::get_mutable().insert(it, std::move(*plugin));
      loaded_plugin_paths.emplace_back(std::move(path));
    } else {
      return std::move(plugin.error());
    }
  }
  // Remove plugins that are explicitly disabled.
  for (auto& plugin : get_mutable()) {
    plugin.reference_dependencies();
  }
  const auto disabled_plugins
    = caf::get_or(cfg, "tenzir.disable-plugins", std::vector<std::string>{""});
  const auto is_disabled_plugin = [&](const auto& plugin) {
    return std::find(disabled_plugins.begin(), disabled_plugins.end(), plugin)
           != disabled_plugins.end();
  };
  auto removed = std::remove_if(get_mutable().begin(), get_mutable().end(),
                                is_disabled_plugin);
  // Remove plugins whose dependencies are not met. We do this in a loop until
  // for one iteratin we do not remove any plugins with unmet dependencies. Not
  // an ideal algorithm, but it's good enough given that we don't expect to have
  // a million plugins loaded.
  while (true) {
    const auto has_unavailable_dependency = [&](const auto& plugin) {
      for (const auto& dependency : plugin.dependencies()) {
        const auto is_dependency = [&](const auto& plugin) {
          return plugin and plugin == dependency;
        };
        if (std::find_if(get_mutable().begin(), removed, is_dependency)
            == removed) {
          return true;
        }
      }
      return false;
    };
    auto it = std::remove_if(get_mutable().begin(), removed,
                             has_unavailable_dependency);
    if (it == removed) {
      break;
    }
    removed = it;
  }
  get_mutable().erase(removed, get_mutable().end());
  // Sort loaded plugins by name (case-insensitive).
  std::sort(get_mutable().begin(), get_mutable().end());
  return loaded_plugin_paths;
}

/// Initialize loaded plugins.
caf::error initialize(caf::actor_system_config& cfg) {
  // If everything went well, we should have a strictly-ordered list of plugins.
  if (auto it = std::ranges::adjacent_find(get(), std::greater_equal{});
      it != get().end()) {
    auto name_a = (*it)->name();
    ++it;
    auto name_b = (*it)->name();
    if (name_a == name_b) {
      TENZIR_ASSERT(false,
                    fmt::format("found multiple plugins named `{}`", name_a));
    } else {
      TENZIR_ASSERT(false, fmt::format("unexpected plugin ordering: found `{}` "
                                       "before `{}`",
                                       name_a, name_b));
    }
  }
  auto global_config = record{};
  auto global_opts = caf::content(cfg);
  if (auto global_opts_data = to<record>(global_opts)) {
    global_config = std::move(*global_opts_data);
  } else {
    TENZIR_DEBUG("unable to read global configuration options: {}",
                 global_opts_data.error());
  }
  auto plugins_record = record{};
  if (global_config.contains("plugins")) {
    if (auto* plugins_entry = try_as<record>(&global_config.at("plugins"))) {
      plugins_record = std::move(*plugins_entry);
    }
  }
  TENZIR_DEBUG("collected {} global options for plugin initialization",
               global_config.size());
  for (auto& plugin : get_mutable()) {
    auto merged_config = record{};
    // Try to read the configurations from the merged Tenzir configuration.
    if (plugins_record.contains(plugin->name())) {
      if (auto* plugins_entry
          = try_as<record>(&plugins_record.at(plugin->name()))) {
        merged_config = std::move(*plugins_entry);
      } else {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("configuration for plugin {} "
                                           "contains invalid format",
                                           plugin->name()));
      }
    }
    // Initialize the plugin with the merged configuration.
    if (plugin.type() != plugin_ptr::type::builtin) {
      TENZIR_VERBOSE("initializing the {} plugin with options: {}",
                     plugin->name(), merged_config);
    }
    if (auto err = plugin->initialize(merged_config, global_config)) {
      return diagnostic::error(err)
        .note("failed to initialize the `{}` plugin", plugin->name())
        .to_error();
    }
  }
  return caf::none;
}

const std::vector<std::filesystem::path>& loaded_config_files() {
  return loaded_config_files_singleton;
}

} // namespace plugins

// -- component plugin --------------------------------------------------------

std::string component_plugin::component_name() const {
  return this->name();
}

auto component_plugin::wanted_components() const -> std::vector<std::string> {
  return {};
}

// -- loader plugin -----------------------------------------------------------

auto loader_parser_plugin::supported_uri_schemes() const
  -> std::vector<std::string> {
  return {this->name()};
}

// -- saver plugin ------------------------------------------------------------

auto saver_parser_plugin::supported_uri_schemes() const
  -> std::vector<std::string> {
  return {this->name()};
}

// -- store plugin -------------------------------------------------------------

caf::expected<store_actor_plugin::builder_and_header>
store_plugin::make_store_builder(filesystem_actor fs,
                                 const tenzir::uuid& id) const {
  auto store = make_active_store();
  if (!store) {
    return store.error();
  }
  auto db_dir = std::filesystem::path{
    caf::get_or(content(fs->home_system().config()), "tenzir.state-directory",
                defaults::state_directory.data())};
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  auto path = abs_dir / "archive" / fmt::format("{}.{}", id, name());
  auto store_builder = fs->home_system().spawn<caf::lazy_init>(
    default_active_store, std::move(*store), fs, std::move(path), name());
  auto header = chunk::copy(id);
  return builder_and_header{store_builder, header};
}

caf::expected<store_actor>
store_plugin::make_store(filesystem_actor fs,
                         std::span<const std::byte> header) const {
  auto store = make_passive_store();
  if (!store) {
    return store.error();
  }
  if (header.size() != uuid::num_bytes) {
    return caf::make_error(ec::invalid_argument, "header must have size of "
                                                 "single uuid");
  }
  const auto id = uuid{header.subspan<0, uuid::num_bytes>()};
  auto db_dir = std::filesystem::path{
    caf::get_or(content(fs->home_system().config()), "tenzir.state-directory",
                defaults::state_directory.data())};
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  auto path = abs_dir / "archive" / fmt::format("{}.{}", id, name());
  return fs->home_system().spawn<caf::lazy_init>(
    default_passive_store, std::move(*store), fs, std::move(path), name());
}

// -- context plugin -----------------------------------------------------------

auto context_plugin::get_latest_loader() const -> const context_loader& {
  TENZIR_ASSERT(not loaders_.empty());
  return **std::ranges::max_element(loaders_, std::ranges::less{},
                                    [](const auto& loader) {
                                      return loader->version();
                                    });
}

auto context_plugin::get_versioned_loader(int version) const
  -> const context_loader* {
  auto it = std::ranges::find(loaders_, version, [](const auto& loader) {
    return loader->version();
  });
  if (it == loaders_.end()) {
    return nullptr;
  }
  return it->get();
}

void context_plugin::register_loader(std::unique_ptr<context_loader> loader) {
  loaders_.emplace_back(std::move(loader));
}

// -- aspect plugin ------------------------------------------------------------

auto aspect_plugin::aspect_name() const -> std::string {
  return name();
}

// -- parser plugin ------------------------------------------------------------

auto plugin_parser::parse_strings(std::shared_ptr<arrow::StringArray> input,
                                  operator_control_plane& ctrl) const
  -> std::vector<series> {
  // TODO: Collecting finished table slices here is very bad for performance.
  // For example, we have to concatenate new table slices. But there are also
  // many questions with regards to semantics. This should be either completely
  // rewritten or replaced with a different mechanism after the revamp.
  auto output = std::vector<table_slice>{};
  auto append_null = [&] {
    if (output.empty()) {
      auto schema = type{"tenzir.unknown", record_type{}};
      output.emplace_back(arrow::RecordBatch::Make(schema.to_arrow_schema(), 1,
                                                   arrow::ArrayVector{}),
                          schema);
      return;
    }
    auto& last = output.back();
    auto null_builder = as<record_type>(last.schema())
                          .make_arrow_builder(arrow::default_memory_pool());
    TENZIR_ASSERT(null_builder->AppendNull().ok());
    auto null_array = std::shared_ptr<arrow::StructArray>{};
    TENZIR_ASSERT(null_builder->Finish(&null_array).ok());
    auto null_batch = arrow::RecordBatch::Make(
      last.schema().to_arrow_schema(), 1, null_array->Flatten().ValueOrDie());
    last
      = concatenate({std::move(last), table_slice{null_batch, last.schema()}});
  };
  for (auto str : values(string_type{}, *input)) {
    if (not str) {
      append_null();
      continue;
    }
    auto bytes = as_bytes(*str);
    auto chunk = chunk::make(bytes, []() noexcept {});
    auto instance = instantiate(
      [](chunk_ptr chunk) -> generator<chunk_ptr> {
        co_yield std::move(chunk);
      }(std::move(chunk)),
      ctrl);
    if (not instance) {
      append_null();
      continue;
    }
    auto slices = collect(std::move(*instance));
    std::erase_if(slices, [](table_slice& x) {
      return x.rows() == 0;
    });
    if (slices.size() != 1) {
      append_null();
      continue;
    }
    auto slice = std::move(slices[0]);
    if (slice.rows() != 1) {
      append_null();
      continue;
    }
    // TODO: Requiring exact schema equality will often produce tiny batches.
    if (not output.empty() && output.back().schema() == slice.schema()) {
      output.back() = concatenate({std::move(output.back()), std::move(slice)});
    } else {
      output.push_back(std::move(slice));
    }
  }
  auto result = std::vector<series>{};
  result.reserve(output.size());
  for (auto&& slice : output) {
    result.emplace_back(slice.schema(),
                        to_record_batch(slice)->ToStructArray().ValueOrDie());
  }
  return result;
}

// -- plugin_ptr ---------------------------------------------------------------

caf::expected<plugin_ptr>
plugin_ptr::make_dynamic(const char* filename,
                         caf::actor_system_config& cfg) noexcept {
  TENZIR_DISABLE_LEAK_SANITIZER();
  auto* library = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY);
  TENZIR_ENABLE_LEAK_SANITIZER();
  if (!library) {
    return caf::make_error(ec::system_error, "failed to load plugin", filename,
                           dlerror());
  }
  auto libtenzir_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_libtenzir_version"));
  if (!libtenzir_version) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "tenzir_libtenzir_version in",
                           filename, dlerror());
  }
  if (strcmp(libtenzir_version(), version::version) != 0) {
    return caf::make_error(ec::version_error, "libtenzir version mismatch in",
                           filename, libtenzir_version(), version::version);
  }
  auto libtenzir_build_tree_hash = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_libtenzir_build_tree_hash"));
  if (!libtenzir_build_tree_hash) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "tenzir_libtenzir_build_tree_hash in",
                           filename, dlerror());
  }
  if (strcmp(libtenzir_build_tree_hash(), version::build::tree_hash) != 0) {
    return caf::make_error(ec::version_error,
                           "libtenzir build tree hash mismatch in", filename,
                           libtenzir_build_tree_hash(),
                           version::build::tree_hash);
  }
  auto plugin_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_plugin_version"));
  if (not plugin_version) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_version in",
                           filename, dlerror());
  }
  auto plugin_dependencies = reinterpret_cast<const char* const* (*)()>(
    dlsym(library, "tenzir_plugin_dependencies"));
  if (not plugin_dependencies) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "tenzir_plugin_dependencies in",
                           filename, dlerror());
  }
  auto dependencies = std::vector<std::string>{};
  if (const char* const* dependencies_clist = plugin_dependencies()) {
    while (const char* dependency = *dependencies_clist++) {
      dependencies.emplace_back(dependency);
    }
  }
  auto plugin_create = reinterpret_cast<::tenzir::plugin* (*)()>(
    dlsym(library, "tenzir_plugin_create"));
  if (not plugin_create) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_create in",
                           filename, dlerror());
  }
  auto plugin_destroy = reinterpret_cast<void (*)(::tenzir::plugin*)>(
    dlsym(library, "tenzir_plugin_destroy"));
  if (not plugin_destroy) {
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_destroy in",
                           filename, dlerror());
  }
  auto plugin_type_id_block
    = reinterpret_cast<::tenzir::plugin_type_id_block (*)()>(
      dlsym(library, "tenzir_plugin_type_id_block"));
  if (plugin_type_id_block) {
    auto plugin_register_type_id_block
      = reinterpret_cast<void (*)(::caf::actor_system_config&)>(
        dlsym(library, "tenzir_plugin_register_type_id_block"));
    if (not plugin_register_type_id_block) {
      return caf::make_error(ec::system_error,
                             "failed to resolve symbol "
                             "tenzir_plugin_register_type_id_block in",
                             filename, dlerror());
    }
    // If the plugin requested to add additional type ID blocks, check if the
    // ranges overlap. Since this is static for the whole process, we just
    // store the already registed ID blocks from plugins in a static variable.
    static auto old_blocks = std::vector<::tenzir::plugin_type_id_block>{
      {caf::id_block::tenzir_types::begin, caf::id_block::tenzir_actors::end}};
    // Static plugins are built as part of the tenzir binary rather then
    // libtenzir, so there will be runtime errors when there is a type ID clash
    // between static and dynamic plugins. We register the ID blocks of all
    // static plugins exactly once to always prefer them over dynamic plugins.
    static auto flag = std::once_flag{};
    std::call_once(flag, [&] {
      for (const auto& [block, _] : plugins::get_static_type_id_blocks()) {
        old_blocks.push_back(block);
      }
    });
    auto new_block = plugin_type_id_block();
    for (const auto& old_block : old_blocks) {
      if (new_block.begin < old_block.end && old_block.begin < new_block.end) {
        return caf::make_error(ec::system_error,
                               "encountered type ID block clash in", filename);
      }
    }
    plugin_register_type_id_block(cfg);
    old_blocks.push_back(new_block);
  }
  return plugin_ptr{
    std::make_shared<control_block>(library, plugin_create(), plugin_destroy,
                                    plugin_version(), std::move(dependencies),
                                    type::dynamic),
  };
}

plugin_ptr
plugin_ptr::make_static(plugin* instance, void (*deleter)(plugin*),
                        const char* version,
                        std::vector<std::string> dependencies) noexcept {
  return plugin_ptr{
    std::make_shared<control_block>(nullptr, instance, deleter, version,
                                    std::move(dependencies), type::static_),
  };
}

plugin_ptr
plugin_ptr::make_builtin(plugin* instance, void (*deleter)(plugin*),
                         const char* version,
                         std::vector<std::string> dependencies) noexcept {
  return plugin_ptr{
    std::make_shared<control_block>(nullptr, instance, deleter, version,
                                    std::move(dependencies), type::builtin),
  };
}

plugin_ptr::plugin_ptr() noexcept = default;
plugin_ptr::~plugin_ptr() noexcept = default;
plugin_ptr::plugin_ptr(plugin_ptr&& other) noexcept = default;
plugin_ptr& plugin_ptr::operator=(plugin_ptr&& rhs) noexcept = default;

plugin_ptr::operator bool() const noexcept {
  return ctrl_ and ctrl_->instance;
}

const plugin* plugin_ptr::operator->() const noexcept {
  return ctrl_->instance;
}

plugin* plugin_ptr::operator->() noexcept {
  return ctrl_->instance;
}

const plugin& plugin_ptr::operator*() const noexcept {
  return *ctrl_->instance;
}

plugin& plugin_ptr::operator&() noexcept {
  return *ctrl_->instance;
}

const char* plugin_ptr::version() const noexcept {
  return ctrl_->version;
}

const std::vector<std::string>& plugin_ptr::dependencies() const noexcept {
  return ctrl_->dependencies;
}

enum plugin_ptr::type plugin_ptr::type() const noexcept {
  return ctrl_->type;
}

auto plugin_ptr::reference_dependencies() noexcept -> void {
  for (const auto& dependency : dependencies()) {
    for (const auto& plugin : plugins::get()) {
      if (plugin == dependency) {
        ctrl_->dependencies_ctrl.push_back(plugin.ctrl_);
      }
    }
  }
}

std::strong_ordering
operator<=>(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept {
  if (&lhs == &rhs) {
    return std::strong_ordering::equal;
  }
  if (!lhs && !rhs) {
    return std::strong_ordering::equal;
  }
  if (!lhs) {
    return std::strong_ordering::less;
  }
  if (!rhs) {
    return std::strong_ordering::greater;
  }
  return lhs <=> rhs->name();
}

bool operator==(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept {
  if (&lhs == &rhs) {
    return true;
  }
  if (!lhs && !rhs) {
    return true;
  }
  return lhs == rhs->name();
}

std::strong_ordering
operator<=>(const plugin_ptr& lhs, std::string_view rhs) noexcept {
  if (!lhs) {
    return std::strong_ordering::less;
  }
  auto lhs_name = lhs->name();
  // TODO: Replace implementation with `std::lexicographical_compare_three_way`
  // once that is implemented for all compilers we need to support. This does
  // the same thing essentially, just a lot less generic.
  while (!lhs_name.empty() && !rhs.empty()) {
    const auto lhs_normalized
      = std::tolower(static_cast<unsigned char>(lhs_name[0]));
    const auto rhs_normalized
      = std::tolower(static_cast<unsigned char>(rhs[0]));
    if (lhs_normalized < rhs_normalized) {
      return std::strong_ordering::less;
    }
    if (lhs_normalized > rhs_normalized) {
      return std::strong_ordering::greater;
    }
    lhs_name = lhs_name.substr(1);
    rhs = rhs.substr(1);
  }
  return !lhs_name.empty() ? std::strong_ordering::greater
         : !rhs.empty()    ? std::strong_ordering::less
                           : std::strong_ordering::equivalent;
}

bool operator==(const plugin_ptr& lhs, std::string_view rhs) noexcept {
  if (!lhs) {
    return false;
  }
  const auto lhs_name = lhs->name();
  return std::equal(lhs_name.begin(), lhs_name.end(), rhs.begin(), rhs.end(),
                    [](unsigned char lhs, unsigned char rhs) noexcept {
                      return std::tolower(lhs) == std::tolower(rhs);
                    });
}

plugin_ptr::control_block::control_block(void* library, plugin* instance,
                                         void (*deleter)(plugin*),
                                         const char* version,
                                         std::vector<std::string> dependencies,
                                         enum type type) noexcept
  : library{library},
    instance{instance},
    deleter{deleter},
    version{version},
    dependencies{std::move(dependencies)},
    type{type} {
  // nop
}

plugin_ptr::control_block::~control_block() noexcept {
  if (instance) {
    TENZIR_ASSERT(deleter);
    deleter(instance);
    instance = {};
    deleter = {};
  }
  if (library) {
    dlclose(library);
    library = {};
  }
  version = {};
  dependencies = {};
  type = {};
}

plugin_ptr::plugin_ptr(std::shared_ptr<control_block> ctrl) noexcept
  : ctrl_{std::move(ctrl)} {
  // nop
}

} // namespace tenzir
