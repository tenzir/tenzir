//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/taxonomies.hpp"

#include <caf/actor_system_config.hpp>

namespace tenzir::modules {

/// Initialize the global module and concepts registries.
///
/// Must be called at most once.
void init(const caf::actor_system_config& cfg, symbol_map mod,
          concepts_map concepts);

/// Returns the schema with the given name, if it exists.
///
/// This function lazily convert the schema definitions we have to `type`,
/// because it turned out that this currently is a bottleneck during startup.
auto get_schema(std::string_view name) -> std::optional<type>;

/// Returns all schemas.
///
/// This function convert all schema definitions we have to `type`, which is
/// rather expensive.
auto expensive_get_all_schemas() -> std::unordered_set<type>;

/// Get the concepts map.
/// @returns An empty map if init(...) was not called.
auto concepts() -> const concepts_map&;

} // namespace tenzir::modules
