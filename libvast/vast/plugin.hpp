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

#include "vast/fwd.hpp"

#include <caf/error.hpp>
#include <caf/stream.hpp>
#include <caf/typed_actor.hpp>

#include <cstdint>
#include <memory>

namespace vast {

extern "C" struct plugin_version {
  uint16_t major;
  uint16_t minor;
  uint16_t patch;
  uint16_t tweak;

  friend bool operator<=(const plugin_version& lhs, const plugin_version& rhs);
};

/// The minimal actor interface that streaming plugins must implement.
/// @relates plugin
using stream_processor
  = caf::typed_actor<caf::reacts_to<caf::stream<table_slice>>>;

class plugin;

/// The plugin base class.
class plugin {
public:
  constexpr static auto version = plugin_version{0, 1, 0, 0};

  /// Destroys any runtime state that the plugin created. For example,
  /// de-register from existing components, deallocate memory.
  virtual ~plugin() = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  virtual caf::error initialize(data config) = 0;

  /// Returns the unique name of the plugin.
  virtual const char* name() const = 0;

  // FIXME: these should go into a sub-class or some category-specific mode.
  // -- plugin-type-specific hooks -------------------------------------------
  virtual stream_processor make_stream_processor(caf::actor_system&) const {
    return stream_processor{};
  }
};

/// @relates plugin
class plugin_ptr final {
public:
  explicit plugin_ptr(const char* filename);
  ~plugin_ptr() noexcept;

  plugin_ptr(const plugin_ptr&) = delete;
  plugin_ptr& operator=(const plugin_ptr&) = delete;

  plugin_ptr(plugin_ptr&&);
  plugin_ptr& operator=(plugin_ptr&&);

  explicit operator bool() noexcept;
  const plugin* operator->() const noexcept;
  plugin* operator->() noexcept;
  const plugin& operator*() const noexcept;
  plugin& operator&() noexcept;

private:
  void* library_ = {};
  plugin* instance_ = {};
  void (*deleter_)(plugin*) = {};
};

} // namespace vast

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
