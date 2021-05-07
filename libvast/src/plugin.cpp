//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/plugin.hpp"

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/node.hpp"

#include <caf/expected.hpp>

#include <dlfcn.h>
#include <memory>
#include <tuple>

namespace vast {

// -- plugin version -----------------------------------------------------------

std::string to_string(plugin_version x) {
  using std::to_string;
  std::string result;
  result += to_string(x.major);
  result += '.';
  result += to_string(x.minor);
  result += '.';
  result += to_string(x.patch);
  result += '-';
  result += to_string(x.tweak);
  return result;
}

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

std::vector<plugin_ptr>& get() noexcept {
  static auto plugins = std::vector<plugin_ptr>{};
  return plugins;
}

std::vector<std::pair<plugin_type_id_block, void (*)(caf::actor_system_config&)>>&
get_static_type_id_blocks() noexcept {
  static auto result = std::vector<
    std::pair<plugin_type_id_block, void (*)(caf::actor_system_config&)>>{};
  return result;
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
  auto plugin_version = reinterpret_cast<::vast::plugin_version (*)()>(
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
                                   plugin_version version) noexcept {
  return plugin_ptr{nullptr, instance, deleter, version, type::static_};
}

plugin_ptr plugin_ptr::make_native(plugin* instance, void (*deleter)(plugin*),
                                   plugin_version version) noexcept {
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
                       void (*deleter)(plugin*), plugin_version version,
                       enum type type) noexcept
  : library_{library},
    instance_{instance},
    deleter_{deleter},
    version_{version},
    type_{type} {
  // nop
}

const plugin_version& plugin_ptr::version() const noexcept {
  return version_;
}

enum plugin_ptr::type plugin_ptr::type() const noexcept {
  return type_;
}

} // namespace vast
