//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"

#include "vast/chunk.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/installdirs.hpp"
#include "vast/detail/settings.hpp"
#include "vast/detail/stable_set.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/operator_control_plane.hpp"
#include "vast/plugin.hpp"
#include "vast/query_context.hpp"
#include "vast/store.hpp"
#include "vast/system/configuration.hpp"
#include "vast/system/node.hpp"
#include "vast/uuid.hpp"

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
  if (auto dirs = detail::unpack_config_list_to_vector<std::string>( //
        cfg, "vast.plugin-dirs"))
    result.insert(dirs->begin(), dirs->end());
  else
    VAST_WARN("failed to to extract plugin dirs: {}", dirs.error());
  if (!bare_mode)
    if (auto home = detail::getenv("HOME"))
      result.insert(std::filesystem::path{*home} / ".local" / "lib" / "vast"
                    / "plugins");
  result.insert(detail::install_plugindir());
  return result;
}

caf::expected<std::filesystem::path>
resolve_plugin_name(const detail::stable_set<std::filesystem::path>& plugin_dirs,
                    std::string_view name) {
  auto plugin_file_name
    = fmt::format("libvast-plugin-{}.{}", name, VAST_MACOS ? "dylib" : "so");
  for (const auto& dir : plugin_dirs) {
    auto maybe_path = dir / plugin_file_name;
    auto ec = std::error_code{};
    if (std::filesystem::is_regular_file(maybe_path, ec))
      return maybe_path;
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
        if (ec || !file.is_regular_file())
          break;
        if (file.path().filename().string().starts_with("libvast-plugin-"))
          paths_or_names.push_back(file.path());
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
    die("unreachable");
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
  // Deduplicate plugin paths.
  auto paths = detail::stable_set<std::string>{};
  paths.insert(std::make_move_iterator(paths_or_names.begin()),
               std::make_move_iterator(paths_or_names.end()));
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
    = caf::get_or(cfg, "vast.plugins", std::vector<std::string>{"all"});
  if (paths_or_names.empty() && bundled_plugins.empty())
    return loaded_plugin_paths;
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
  if (!paths)
    return std::move(paths.error());
  // Load plugins.
  for (auto path : std::move(*paths)) {
    if (auto plugin = plugin_ptr::make_dynamic(path.c_str(), cfg)) {
      // Check for name clashes.
      if (find((*plugin)->name()))
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
  // Sort loaded plugins by name (case-insensitive).
  std::sort(get_mutable().begin(), get_mutable().end());
  return loaded_plugin_paths;
}

/// Initialize loaded plugins.
caf::error initialize(caf::actor_system_config& cfg) {
  auto global_config = record{};
  auto global_opts = caf::content(cfg);
  if (auto global_opts_data = to<record>(global_opts)) {
    global_config = std::move(*global_opts_data);
  } else {
    VAST_DEBUG("unable to read global configuration options: {}",
               global_opts_data.error());
  }
  auto plugins_record = record{};
  if (global_config.contains("plugins")) {
    if (auto plugins_entry
        = caf::get_if<record>(&global_config.at("plugins"))) {
      plugins_record = std::move(*plugins_entry);
    }
  }
  VAST_DEBUG("collected {} global options for plugin initialization",
             global_config.size());
  std::vector<std::string> disabled_plugins;
  for (auto& plugin : get_mutable()) {
    auto merged_config = record{};
    // First, try to read the configurations from the merged VAST configuration.
    if (plugins_record.contains(plugin->name())) {
      if (auto plugins_entry
          = caf::get_if<record>(&plugins_record.at(plugin->name()))) {
        merged_config = std::move(*plugins_entry);
      } else {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("configuration for plugin {} "
                                           "contains invalid format",
                                           plugin->name()));
      }
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
          fmt::format("detected configuration files for the "
                      "{} plugin at "
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
    // Allow the plugin to control whether it should be loaded based on
    // its configuration.
    if (!plugin->enabled(merged_config, global_config)) {
      VAST_VERBOSE("disabling plugin {}", plugin->name());
      disabled_plugins.push_back(plugin->name());
      continue;
    }
    // Third, initialize the plugin with the merged configuration.
    VAST_VERBOSE("initializing the {} plugin with options: {}", plugin->name(),
                 merged_config);
    if (auto err = plugin->initialize(merged_config, global_config))
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to initialize "
                                         "the {} plugin: {} ",
                                         plugin->name(), err));
  }
  for (auto const& plugin_name : disabled_plugins) {
    auto& plugins = get_mutable();
    auto position
      = std::find_if(plugins.begin(), plugins.end(), [=](const plugin_ptr& p) {
          return p->name() == plugin_name;
        });
    VAST_ASSERT_CHEAP(position != plugins.end());
    plugins.erase(position);
  }
  return caf::none;
}

const std::vector<std::filesystem::path>& loaded_config_files() {
  return loaded_config_files_singleton;
}

} // namespace plugins

// -- plugin base class -------------------------------------------------------

bool plugin::enabled(const record&, const record& plugin_config) const {
  auto default_value = true;
  auto result = try_get_or(plugin_config, "enabled", default_value);
  if (!result) {
    VAST_WARN("config option {}.enabled is ignored: expected a boolean",
              this->name());
    return default_value;
  }
  return *result;
}

// -- component plugin --------------------------------------------------------

std::string component_plugin::component_name() const {
  return this->name();
}

// -- analyzer plugin ---------------------------------------------------------

system::analyzer_plugin_actor analyzer_plugin::analyzer(
  system::node_actor::stateful_pointer<system::node_state> node) const {
  if (auto handle = weak_handle_.lock())
    return handle;
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
  weak_handle_ = handle;
  spawned_once_ = true;
  return handle;
}

system::component_plugin_actor analyzer_plugin::make_component(
  system::node_actor::stateful_pointer<system::node_state> node) const {
  return analyzer(node);
}

// -- store plugin -------------------------------------------------------------

caf::expected<store_actor_plugin::builder_and_header>
store_plugin::make_store_builder(system::accountant_actor accountant,
                                 system::filesystem_actor fs,
                                 const vast::uuid& id) const {
  auto store = make_active_store();
  if (!store)
    return store.error();
  auto path
    = std::filesystem::path{"archive"} / fmt::format("{}.{}", id, name());
  auto store_builder = fs->home_system().spawn<caf::lazy_init>(
    default_active_store, std::move(*store), fs, std::move(accountant),
    std::move(path), name());
  auto header = chunk::copy(id);
  return builder_and_header{store_builder, header};
}

caf::expected<system::store_actor>
store_plugin::make_store(system::accountant_actor accountant,
                         system::filesystem_actor fs,
                         std::span<const std::byte> header) const {
  auto store = make_passive_store();
  if (!store)
    return store.error();
  if (header.size() != uuid::num_bytes)
    return caf::make_error(ec::invalid_argument, "header must have size of "
                                                 "single uuid");
  const auto id = uuid{header.subspan<0, uuid::num_bytes>()};
  auto path
    = std::filesystem::path{"archive"} / fmt::format("{}.{}", id, name());
  return fs->home_system().spawn<caf::lazy_init>(default_passive_store,
                                                 std::move(*store), fs,
                                                 std::move(accountant),
                                                 std::move(path), name());
}

auto store_plugin::make_parser(std::vector<std::string> args,
                               generator<chunk_ptr> loader,
                               operator_control_plane& ctrl) const
  -> caf::expected<parser> {
  if (not args.empty()) {
    return caf::make_error(ec::invalid_argument,
                           fmt ::format("{} parser expected no arguments, but "
                                        "got [{}]",
                                        name(), fmt::join(args, ", ")));
  }
  auto store = make_passive_store();
  if (not store) {
    return caf::make_error(ec::logic_error,
                           fmt ::format("{} parser failed to create store: {}",
                                        name(), store.error()));
  }
  return [](generator<chunk_ptr> loader, operator_control_plane& ctrl,
            std::unique_ptr<passive_store> store,
            std::string name) -> generator<table_slice> {
    // TODO: Loading everything into memory here is far from ideal. We should
    // instead load passive stores incrementally. For now we at least warn the
    // user that this is experimental.
    auto chunks = std::vector<chunk_ptr>{};
    for (auto&& chunk : loader) {
      if (not chunk || chunk->size() == 0) {
        co_yield {};
        continue;
      }
      chunks.push_back(std::move(chunk));
      co_yield {};
    }
    if (chunks.size() == 1) {
      if (auto err = store->load(std::move(chunks.front()))) {
        ctrl.abort(caf::make_error(ec::format_error,
                                   "{} parser failed to load: {}", name,
                                   std::move(err)));
        co_return;
      }
    } else {
      ctrl.warn(caf::make_error(
        ec::unspecified, fmt::format("the experimental {} parser does "
                                     "not currently load files "
                                     "incrementally and may use an "
                                     "excessive amount of memory; consider "
                                     "using 'from file --mmap'",
                                     name)));
      auto buffer = std::vector<std::byte>{};
      for (auto&& chunk : chunks) {
        buffer.insert(buffer.end(), chunk->begin(), chunk->end());
      }
      if (auto err = store->load(chunk::make(std::move(buffer)))) {
        ctrl.abort(caf::make_error(ec::format_error,
                                   "{} parser failed to load: {}", name,
                                   std::move(err)));
        co_return;
      }
    }
    for (auto&& slice : store->slices()) {
      co_yield std::move(slice);
    }
  }(std::move(loader), ctrl, std::move(*store), name());
}

auto store_plugin::default_loader(std::span<std::string const>) const
  -> std::pair<std::string, std::vector<std::string>> {
  return {"stdin", {}};
}

auto store_plugin::make_printer(std::span<std::string const> args,
                                type input_schema,
                                operator_control_plane& ctrl) const
  -> caf::expected<printer> {
  VAST_ASSERT(input_schema != type{});
  if (not args.empty()) {
    return caf::make_error(ec::invalid_argument,
                           fmt ::format("{} printer expected no arguments, but "
                                        "got [{}]",
                                        name(), fmt::join(args, ", ")));
  }
  auto store = make_active_store();
  if (not store) {
    return caf::make_error(ec::logic_error,
                           fmt ::format("{} parser failed to create store: {}",
                                        name(), store.error()));
  }

  class store_printer : public printer_base {
  public:
    explicit store_printer(std::unique_ptr<active_store> store,
                           operator_control_plane& ctrl)
      : store_{std::move(store)}, ctrl_{ctrl} {
    }

    auto process(table_slice slice) -> generator<chunk_ptr> override {
      auto vec = std::vector<table_slice>{};
      vec.push_back(std::move(slice));
      if (auto error = store_->add(std::move(vec))) {
        ctrl_.abort(std::move(error));
        co_return;
      }
      // TODO
      auto chunk = store_->finish();
      if (!chunk) {
        ctrl_.abort(std::move(chunk.error()));
        co_return;
      }
      co_yield std::move(*chunk);
    }

    auto finish() -> generator<chunk_ptr> override {
      return {};
    }

  private:
    std::unique_ptr<active_store> store_;
    operator_control_plane& ctrl_;
  };

  return std::make_unique<store_printer>(std::move(*store), ctrl);
}

auto store_plugin::default_saver(std::span<std::string const>) const
  -> std::pair<std::string, std::vector<std::string>> {
  return {"directory", {"."}};
}

auto store_plugin::printer_allows_joining() const -> bool {
  return false;
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
  if (strcmp(libvast_build_tree_hash(), version::build::tree_hash) != 0)
    return caf::make_error(ec::version_error,
                           "libvast build tree hash mismatch in", filename,
                           libvast_build_tree_hash(),
                           version::build::tree_hash);
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

plugin_ptr plugin_ptr::make_builtin(plugin* instance, void (*deleter)(plugin*),
                                    const char* version) noexcept {
  return plugin_ptr{nullptr, instance, deleter, version, type::builtin};
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

std::strong_ordering
operator<=>(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept {
  if (&lhs == &rhs)
    return std::strong_ordering::equal;
  if (!lhs && !rhs)
    return std::strong_ordering::equal;
  if (!lhs)
    return std::strong_ordering::less;
  if (!rhs)
    return std::strong_ordering::greater;
  return lhs <=> rhs->name();
}

bool operator==(const plugin_ptr& lhs, const plugin_ptr& rhs) noexcept {
  if (&lhs == &rhs)
    return true;
  if (!lhs && !rhs)
    return true;
  return lhs == rhs->name();
}

std::strong_ordering
operator<=>(const plugin_ptr& lhs, std::string_view rhs) noexcept {
  if (!lhs)
    return std::strong_ordering::less;
  auto lhs_name = lhs->name();
  // TODO: Replace implementation with `std::lexicographical_compare_three_way`
  // once that is implemented for all compilers we need to support. This does
  // the same thing essentially, just a lot less generic.
  while (!lhs_name.empty() && !rhs.empty()) {
    const auto lhs_normalized
      = std::tolower(static_cast<unsigned char>(lhs_name[0]));
    const auto rhs_normalized
      = std::tolower(static_cast<unsigned char>(rhs[0]));
    if (lhs_normalized < rhs_normalized)
      return std::strong_ordering::less;
    if (lhs_normalized > rhs_normalized)
      return std::strong_ordering::greater;
    lhs_name = lhs_name.substr(1);
    rhs = rhs.substr(1);
  }
  return !lhs_name.empty() ? std::strong_ordering::greater
         : !rhs.empty()    ? std::strong_ordering::less
                           : std::strong_ordering::equivalent;
}

bool operator==(const plugin_ptr& lhs, std::string_view rhs) noexcept {
  if (!lhs)
    return false;
  const auto lhs_name = lhs->name();
  return std::equal(lhs_name.begin(), lhs_name.end(), rhs.begin(), rhs.end(),
                    [](unsigned char lhs, unsigned char rhs) noexcept {
                      return std::tolower(lhs) == std::tolower(rhs);
                    });
}

} // namespace vast
