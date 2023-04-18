//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/taxonomies.hpp"

namespace vast::modules {

/// Initialize the global module and concepts registries.
/// @returns `true` after first initialization, `false` otherwise.
auto init(module mod, concepts_map concepts) -> bool;

/// Get the list of schemas.
/// @returns An empty list if init(...) was not called.
auto schemas() -> const std::vector<type>&;

/// Get the concepts map.
/// @returns An empty map if init(...) was not called.
auto concepts() -> const concepts_map&;

/// Get the list of modules.
[[deprecated("call modules::schemas() instead")]] auto global_module()
  -> const module*;

} // namespace vast::modules
