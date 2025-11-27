//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/module.hpp"
#include "tenzir/taxonomies.hpp"

namespace tenzir::modules {

/// Initialize the global module and concepts registries.
///
/// Must be called at most once.
void init(symbol_map mod, symbol_map2 mod2, concepts_map concepts);

/// Returns the schema with the given name, if it exists.
///
/// This function lazily convert the schema definitions we have to `type`,
/// because it turned out that this currently is a bottleneck during startup.
auto get_schema(std::string_view name) -> std::optional<type>;

/// Get the concepts map.
/// @returns An empty map if init(...) was not called.
auto concepts() -> const concepts_map&;

} // namespace tenzir::modules
