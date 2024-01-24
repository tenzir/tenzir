//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugin.hpp"

#include "tenzir/argument_parser.hpp"
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
#include "tenzir/node.hpp"
#include "tenzir/operator_control_plane.hpp"
#include "tenzir/store.hpp"
#include "tenzir/uuid.hpp"

#include <arrow/api.h>
#include <caf/actor_system_config.hpp>
#include <caf/expected.hpp>

#include <algorithm>
#include <dlfcn.h>
#include <memory>
#include <tuple>
#include <unordered_set>

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
        cfg, "tenzir.plugin-dirs"))
    result.insert(dirs->begin(), dirs->end());
  else
    TENZIR_WARN("failed to to extract plugin dirs: {}", dirs.error());
  if (!bare_mode)
    if (auto home = detail::getenv("HOME"))
      result.insert(std::filesystem::path{*home} / ".local" / "lib" / "tenzir"
                    / "plugins");
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
        if (file.path().filename().string().starts_with("libtenzir-plugin-"))
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
    // `{dir}/libtenzir-plugin-{name}.{ext}`. We take the first file that
    // exists.
    if (auto path = resolve_plugin_name(plugin_dirs, path_or_name))
      path_or_name = path->string();
    else
      return std::move(path.error());
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
  // Remove plugins that are explicitly disabled.
  const auto disabled_plugins
    = caf::get_or(cfg, "tenzir.disable-plugins", std::vector<std::string>{""});
  const auto is_disabled_plugin = [&](const auto& plugin) {
    return std::find(disabled_plugins.begin(), disabled_plugins.end(), plugin)
           != disabled_plugins.end();
  };
  get_mutable().erase(std::remove_if(get_mutable().begin(), get_mutable().end(),
                                     is_disabled_plugin),
                      get_mutable().end());
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
    TENZIR_DEBUG("unable to read global configuration options: {}",
                 global_opts_data.error());
  }
  auto plugins_record = record{};
  if (global_config.contains("plugins")) {
    if (auto plugins_entry
        = caf::get_if<record>(&global_config.at("plugins"))) {
      plugins_record = std::move(*plugins_entry);
    }
  }
  TENZIR_DEBUG("collected {} global options for plugin initialization",
               global_config.size());
  for (auto& plugin : get_mutable()) {
    auto merged_config = record{};
    // First, try to read the configurations from the merged Tenzir configuration.
    if (plugins_record.contains(plugin->name())) {
      if (auto* plugins_entry
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
    for (auto&& config_dir : config_dirs(cfg)) {
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
        if (const auto& opts_data = caf::get_if<record>(&*opts)) {
          merge(*opts_data, merged_config, policy::merge_lists::yes);
          TENZIR_INFO("loaded plugin configuration file: {}", path);
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
    if (plugin.type() != plugin_ptr::type::builtin)
      TENZIR_VERBOSE("initializing the {} plugin with options: {}",
                     plugin->name(), merged_config);
    if (auto err = plugin->initialize(merged_config, global_config))
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to initialize "
                                         "the {} plugin: {} ",
                                         plugin->name(), err));
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

// -- analyzer plugin ---------------------------------------------------------

analyzer_plugin_actor
analyzer_plugin::analyzer(node_actor::stateful_pointer<node_state> node) const {
  if (auto handle = weak_handle_.lock())
    return handle;
  if (spawned_once_ || !node)
    return {};
  auto handle = make_analyzer(node);
  auto [importer] = node->state.registry.find<importer_actor>();
  TENZIR_ASSERT(importer);
  node
    ->request(importer, caf::infinite,
              static_cast<stream_sink_actor<table_slice>>(handle))
    .then([](const caf::outbound_stream_slot<table_slice>&) {},
          [&](const caf::error& error) {
            TENZIR_ERROR("failed to connect analyzer {} to the importer: {}",
                         name(), error);
          });
  weak_handle_ = handle;
  spawned_once_ = true;
  return handle;
}

component_plugin_actor analyzer_plugin::make_component(
  node_actor::stateful_pointer<node_state> node) const {
  return analyzer(node);
}

// -- loader plugin -----------------------------------------------------------

auto loader_parser_plugin::supported_uri_scheme() const -> std::string {
  return this->name();
}

// -- saver plugin ------------------------------------------------------------

auto saver_parser_plugin::supported_uri_scheme() const -> std::string {
  return this->name();
}

// -- store plugin -------------------------------------------------------------

caf::expected<store_actor_plugin::builder_and_header>
store_plugin::make_store_builder(accountant_actor accountant,
                                 filesystem_actor fs,
                                 const tenzir::uuid& id) const {
  auto store = make_active_store();
  if (!store)
    return store.error();
  auto db_dir = std::filesystem::path{
    caf::get_or(content(fs->home_system().config()), "tenzir.db-directory",
                defaults::db_directory.data())};
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  auto path = abs_dir / "archive" / fmt::format("{}.{}", id, name());
  auto store_builder = fs->home_system().spawn<caf::lazy_init>(
    default_active_store, std::move(*store), fs, std::move(accountant),
    std::move(path), name());
  auto header = chunk::copy(id);
  return builder_and_header{store_builder, header};
}

caf::expected<store_actor>
store_plugin::make_store(accountant_actor accountant, filesystem_actor fs,
                         std::span<const std::byte> header) const {
  auto store = make_passive_store();
  if (!store)
    return store.error();
  if (header.size() != uuid::num_bytes)
    return caf::make_error(ec::invalid_argument, "header must have size of "
                                                 "single uuid");
  const auto id = uuid{header.subspan<0, uuid::num_bytes>()};
  auto db_dir = std::filesystem::path{
    caf::get_or(content(fs->home_system().config()), "tenzir.db-directory",
                defaults::db_directory.data())};
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  auto path = abs_dir / "archive" / fmt::format("{}.{}", id, name());
  return fs->home_system().spawn<caf::lazy_init>(default_passive_store,
                                                 std::move(*store), fs,
                                                 std::move(accountant),
                                                 std::move(path), name());
}

static auto
store_parser_impl(generator<chunk_ptr> loader, operator_control_plane& ctrl,
                  std::unique_ptr<passive_store> store, std::string name)
  -> generator<table_slice> {
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
      diagnostic::error(err)
        .note("parser failed to load")
        .emit(ctrl.diagnostics());
      co_return;
    }
  } else {
    diagnostic::warning("loading files incrementally is not currently "
                        "supported; parser may use excessive amounts of memory")
      .hint("consider using `from file --mmap` to load the file")
      .emit(ctrl.diagnostics());
    auto buffer = std::vector<std::byte>{};
    for (auto&& chunk : chunks) {
      buffer.insert(buffer.end(), chunk->begin(), chunk->end());
    }
    if (auto err = store->load(chunk::make(std::move(buffer)))) {
      diagnostic::error(err)
        .note("parser failed to load")
        .emit(ctrl.diagnostics());
      co_return;
    }
  }
  for (auto&& slice : store->slices()) {
    co_yield std::move(slice);
  }
}

class store_parser final : public plugin_parser {
public:
  store_parser(const store_plugin* plugin) : plugin_{plugin} {
  }

  auto name() const -> std::string override {
    return plugin_->name();
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    auto store = plugin_->make_passive_store();
    if (!store) {
      diagnostic::error(store.error()).emit(ctrl.diagnostics());
      return {};
    }
    return store_parser_impl(std::move(input), ctrl, std::move(*store),
                             plugin_->name());
  }

private:
  const store_plugin* plugin_;
};

class store_printer_impl : public printer_instance {
public:
  explicit store_printer_impl(std::unique_ptr<active_store> store,
                              operator_control_plane& ctrl)
    : store_{std::move(store)}, ctrl_{ctrl} {
  }

  auto process(table_slice slice) -> generator<chunk_ptr> override {
    auto vec = std::vector<table_slice>{};
    vec.push_back(std::move(slice));
    if (auto error = store_->add(std::move(vec))) {
      diagnostic::error(error)
        .note("printer failed to add")
        .emit(ctrl_.diagnostics());
      co_return;
    }
    // TODO
    auto chunk = store_->finish();
    if (!chunk) {
      diagnostic::error(chunk.error())
        .note("printer failed to finish")
        .emit(ctrl_.diagnostics());
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

class store_printer final : public plugin_printer {
public:
  store_printer(const store_plugin* plugin) : plugin_{plugin} {
  }

  auto instantiate(type input_schema, operator_control_plane& ctrl) const
    -> caf::expected<std::unique_ptr<printer_instance>> override {
    TENZIR_ASSERT(input_schema != type{});
    auto store = plugin_->make_active_store();
    if (not store) {
      return caf::make_error(ec::logic_error,
                             fmt::format("{} parser failed to create store: {}",
                                         name(), store.error()));
    }
    return std::make_unique<store_printer_impl>(std::move(*store), ctrl);
  }

  auto allows_joining() const -> bool override {
    return false;
  }

  auto name() const -> std::string override {
    return plugin_->name();
  }

private:
  const store_plugin* plugin_;
};

auto store_plugin::parse_parser(parser_interface& p) const
  -> std::unique_ptr<plugin_parser> {
  argument_parser{name()}.parse(p);
  return std::make_unique<store_parser>(this);
}

auto store_plugin::parse_printer(parser_interface& p) const
  -> std::unique_ptr<plugin_printer> {
  argument_parser{name()}.parse(p);
  return std::make_unique<store_printer>(this);
}

auto store_plugin::serialize(serializer f, const plugin_parser& x) const
  -> bool {
  auto o = [&](auto g) {
    return g.get().object(x).fields();
  };
  return std::visit(o, f);
}

auto store_plugin::deserialize(deserializer f,
                               std::unique_ptr<plugin_parser>& x) const
  -> void {
  auto o = [&](auto g) {
    return g.get().object(x).fields();
  };
  if (std::visit(o, f)) {
    x = std::make_unique<store_parser>(this);
  } else {
    x = nullptr;
  }
}

auto store_plugin::serialize(serializer f, const plugin_printer& x) const
  -> bool {
  auto o = [&](auto g) {
    return g.get().object(x).fields();
  };
  return std::visit(o, f);
}

auto store_plugin::deserialize(deserializer f,
                               std::unique_ptr<plugin_printer>& x) const
  -> void {
  auto o = [&](auto g) {
    return g.get().object(x).fields();
  };
  if (std::visit(o, f)) {
    x = std::make_unique<store_printer>(this);
  } else {
    x = nullptr;
  }
}
// -- aspect plugin ------------------------------------------------------------

auto aspect_plugin::aspect_name() const -> std::string {
  return name();
}

// -- parser plugin ------------------------------------------------------------

auto plugin_parser::parse_strings(std::shared_ptr<arrow::StringArray> input,
                                  operator_control_plane& ctrl) const
  -> std::vector<typed_array> {
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
    auto null_builder = caf::get<record_type>(last.schema())
                          .make_arrow_builder(arrow::default_memory_pool());
    TENZIR_ASSERT_CHEAP(null_builder->AppendNull().ok());
    auto null_array = std::shared_ptr<arrow::StructArray>{};
    TENZIR_ASSERT_CHEAP(null_builder->Finish(&null_array).ok());
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
  auto result = std::vector<typed_array>{};
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
  auto* library = dlopen(filename, RTLD_GLOBAL | RTLD_LAZY);
  if (!library)
    return caf::make_error(ec::system_error, "failed to load plugin", filename,
                           dlerror());
  auto libtenzir_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_libtenzir_version"));
  if (!libtenzir_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "tenzir_libtenzir_version in",
                           filename, dlerror());
  if (strcmp(libtenzir_version(), version::version) != 0)
    return caf::make_error(ec::version_error, "libtenzir version mismatch in",
                           filename, libtenzir_version(), version::version);
  auto libtenzir_build_tree_hash = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_libtenzir_build_tree_hash"));
  if (!libtenzir_build_tree_hash)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol "
                           "tenzir_libtenzir_build_tree_hash in",
                           filename, dlerror());
  if (strcmp(libtenzir_build_tree_hash(), version::build::tree_hash) != 0)
    return caf::make_error(ec::version_error,
                           "libtenzir build tree hash mismatch in", filename,
                           libtenzir_build_tree_hash(),
                           version::build::tree_hash);
  auto plugin_version = reinterpret_cast<const char* (*)()>(
    dlsym(library, "tenzir_plugin_version"));
  if (!plugin_version)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_version in",
                           filename, dlerror());
  auto plugin_create = reinterpret_cast<::tenzir::plugin* (*)()>(
    dlsym(library, "tenzir_plugin_create"));
  if (!plugin_create)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_create in",
                           filename, dlerror());
  auto plugin_destroy = reinterpret_cast<void (*)(::tenzir::plugin*)>(
    dlsym(library, "tenzir_plugin_destroy"));
  if (!plugin_destroy)
    return caf::make_error(ec::system_error,
                           "failed to resolve symbol tenzir_plugin_destroy in",
                           filename, dlerror());
  auto plugin_type_id_block
    = reinterpret_cast<::tenzir::plugin_type_id_block (*)()>(
      dlsym(library, "tenzir_plugin_type_id_block"));
  if (plugin_type_id_block) {
    auto plugin_register_type_id_block
      = reinterpret_cast<void (*)(::caf::actor_system_config&)>(
        dlsym(library, "tenzir_plugin_register_type_id_block"));
    if (!plugin_register_type_id_block)
      return caf::make_error(ec::system_error,
                             "failed to resolve symbol "
                             "tenzir_plugin_register_type_id_block in",
                             filename, dlerror());
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
  release();
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
  // Clean up *this* to prevent leaking an instance_.
  release();
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

void plugin_ptr::release() noexcept {
  if (instance_) {
    TENZIR_ASSERT(deleter_);
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

} // namespace tenzir
