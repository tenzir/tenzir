// SPDX-FileCopyrightText: (c) 2021 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

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
