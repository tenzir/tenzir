/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/command.hpp"
#include "vast/fwd.hpp"

#include <caf/error.hpp>
#include <caf/stream.hpp>
#include <caf/typed_actor.hpp>

#include <cstdint>
#include <memory>
#include <vector>

namespace vast {

// -- plugin singleton ---------------------------------------------------------

namespace plugins {

/// Retrieves the system-wide plugin singleton.
std::vector<plugin_ptr>& get() noexcept;

} // namespace plugins

// -- plugin version -----------------------------------------------------------

/// The version of a plugin in format major.minor.patch.tweak.
extern "C" struct plugin_version {
  uint16_t major;
  uint16_t minor;
  uint16_t patch;
  uint16_t tweak;
};

/// Checks if a version meets the plugin version requirements.
/// @param version The version to compare against the requirements.
bool has_required_version(const plugin_version& version) noexcept;

/// Support CAF type-inspection.
/// @relates plugin_version
template <class Inspector>
auto inspect(Inspector& f, plugin_version& x) ->
  typename Inspector::result_type {
  return f(x.major, x.minor, x.patch, x.tweak);
}

// -- plugin -------------------------------------------------------------------

/// The plugin base class.
class plugin {
public:
  /// The current version of the plugin API. When registering a plugin, set the
  /// corresponding plugin version in the `VAST_REGISTER_PLUGIN` macro.
  constexpr static auto version = plugin_version{0, 1, 0, 0};

  /// Destroys any runtime state that the plugin created. For example,
  /// de-register from existing components, deallocate memory.
  virtual ~plugin() noexcept = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param config The relevant subsection of the configuration.
  virtual caf::error initialize(data config) = 0;

  /// Returns the unique name of the plugin.
  virtual const char* name() const = 0;
};

// -- import plugin ------------------------------------------------------------

/// A base class for plugins that hook into the importer stream.
/// @relates plugin
class import_plugin : public virtual plugin {
public:
  /// The minimal actor interface that streaming plugins must implement.
  using import_stream_sink_actor
    = caf::typed_actor<caf::reacts_to<caf::stream<table_slice>>>;

  /// Creates an actor that hooks into the importer table slice stream.
  /// @param sys The actor system context to spawn the actor in.
  virtual import_stream_sink_actor
  make_import_stream_sink(caf::actor_system& sys) const = 0;
};

// -- command plugin -----------------------------------------------------------

/// A base class for plugins that add commands.
/// @relates plugin
class command_plugin : public virtual plugin {
public:
  /// Creates additional commands.
  virtual std::pair<std::unique_ptr<command>, command::factory>
  make_command() const = 0;
};

// -- plugin_ptr ---------------------------------------------------------------

/// An owned plugin and dynamically loaded plugin.
/// @relates plugin
class plugin_ptr final {
public:
  /// Load a plugin from the specified library filename.
  /// @param filename The filename that's passed to 'dlopen'.
  explicit plugin_ptr(const char* filename) noexcept;

  /// Unload a plugin and its required resources.
  ~plugin_ptr() noexcept;

  /// Forbid copying of plugins.
  plugin_ptr(const plugin_ptr&) = delete;
  plugin_ptr& operator=(const plugin_ptr&) = delete;

  /// Move-construction and move-assignment.
  plugin_ptr(plugin_ptr&& other) noexcept;
  plugin_ptr& operator=(plugin_ptr&& rhs) noexcept;

  /// Pointer facade.
  explicit operator bool() noexcept;
  const plugin* operator->() const noexcept;
  plugin* operator->() noexcept;
  const plugin& operator*() const noexcept;
  plugin& operator&() noexcept;

  /// Upcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to upcast to.
  /// @returns A pointer to the upcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  const Plugin* as() const {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'vast::plugin'");
    return dynamic_cast<const Plugin*>(instance_);
  }

  /// Upcast a plugin to a more specific plugin type.
  /// @tparam Plugin The specific plugin type to try to upcast to.
  /// @returns A pointer to the upcasted plugin, or 'nullptr' on failure.
  template <class Plugin>
  Plugin* as() {
    static_assert(std::is_base_of_v<plugin, Plugin>, "'Plugin' must be derived "
                                                     "from 'vast::plugin'");
    return dynamic_cast<Plugin*>(instance_);
  }

private:
  /// Implementation details.
  void* library_ = {};
  plugin* instance_ = {};
  void (*deleter_)(plugin*) = {};
};

} // namespace vast

// -- helper macros ------------------------------------------------------------

#define VAST_REGISTER_PLUGIN(name, major, minor, tweak, patch)                 \
  extern "C" ::vast::plugin* plugin_create() {                                 \
    return new name;                                                           \
  }                                                                            \
  extern "C" void plugin_destroy(class ::vast::plugin* plugin) {               \
    delete plugin;                                                             \
  }                                                                            \
  extern "C" struct ::vast::plugin_version plugin_version() {                  \
    return {major, minor, tweak, patch};                                       \
  }
