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

#include "vast/detail/stable_set.hpp"

#include <filesystem>

namespace vast::detail {

/// Dynamically load a plugin.
/// @param file A path to a VAST plugin library. If unspecified, appends `.so`
/// or `.dylib` depending on the platform automatically. Relative paths are
/// interpreted relative to all configured plugin directories in order.
/// @param cfg The actor system configuration of VAST for registering additional
/// type ID blocks.
/// @returns A pair consisting of the absolute path of the loaded plugin and a
/// pointer to the loaded plugin, or an error detailing what went wrong.
caf::expected<std::pair<std::filesystem::path, plugin_ptr>>
load_plugin(std::filesystem::path file, caf::actor_system_config& cfg);

} // namespace vast::detail
