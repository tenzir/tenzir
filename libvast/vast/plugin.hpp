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

#include <memory>

namespace vast {

/// The minimal actor interface that streaming plugins must implement.
/// @relates plugin
using stream_processor
  = caf::typed_actor<caf::reacts_to<caf::stream<table_slice>>>;

class plugin;

/// The plugin base class.
class plugin {
public:
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

  plugin_ptr(const plugin_ptr&) = delete;
  plugin_ptr& operator=(const plugin_ptr&) = delete;

  plugin_ptr(plugin_ptr&&) = default;
  plugin_ptr& operator=(plugin_ptr&&) = default;

  ~plugin_ptr() noexcept {
    instance_.reset();
    library_.reset();
  }

  explicit operator bool() noexcept {
    return static_cast<bool>(instance_);
  }

  const plugin* operator->() const noexcept {
    return instance_.get();
  }

  plugin* operator->() noexcept {
    return instance_.get();
  }

  const plugin& operator*() const noexcept {
    return *instance_;
  }

  plugin& operator&() noexcept {
    return *instance_;
  }

private:
  std::unique_ptr<plugin, void (*)(plugin*)> instance_
    = {nullptr, [](plugin*) noexcept {}};
  std::unique_ptr<void, void (*)(void*)> library_
    = {nullptr, [](void*) noexcept {}};
};

} // namespace vast

#define VAST_REGISTER_PLUGIN(name)                                             \
  extern "C" plugin* create_plugin() {                                         \
    return new name;                                                           \
  }                                                                            \
  extern "C" void destroy_plugin(class plugin* plugin) {                       \
    delete plugin;                                                             \
  }
