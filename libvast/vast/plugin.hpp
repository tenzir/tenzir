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
using plugin_ptr = std::unique_ptr<plugin>;

/// The plugin base class.
class plugin {
public:
  /// Entry point called by dlopen.
  static plugin_ptr make();

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

} // namespace vast
