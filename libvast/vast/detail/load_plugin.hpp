//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/stable_set.hpp"

#include <filesystem>

namespace vast::detail {

/// Dynamically load a plugin.
/// @param file_or_name A path to a VAST plugin library, or the name of a plugin.
/// @param cfg The actor system configuration of VAST for registering additional
/// type ID blocks.
/// @returns A pair consisting of the absolute path of the loaded plugin and a
/// pointer to the loaded plugin, or an error detailing what went wrong.
caf::expected<std::pair<std::filesystem::path, plugin_ptr>>
load_plugin(const std::filesystem::path& file_or_name,
            caf::actor_system_config& cfg);

} // namespace vast::detail
