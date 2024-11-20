//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include "tenzir/module.hpp"
#include "tenzir/taxonomies.hpp"

#include <utility>
#include <vector>

namespace tenzir::modules {

namespace {

struct global_module_registry {
  module mod;
  concepts_map concepts;
  std::vector<type> schemas;
};

bool initialized = false;

auto get_impl() -> global_module_registry& {
  static global_module_registry data;
  return data;
}

} // namespace

/// Initialize the global module, concept and schema registries.
auto init(module mod, concepts_map concepts) -> bool {
  if (initialized) [[likely]]
    return false;
  get_impl().mod = std::move(mod);
  get_impl().concepts = std::move(concepts);
  std::copy_if(get_impl().mod.begin(), get_impl().mod.end(),
               std::back_inserter(get_impl().schemas), [](const auto& type) {
                 return not type.name().empty() && is<record_type>(type);
               });
  initialized = true;
  return true;
}

// Get the list of schemas.
// Returns an empty list if init(...) was not called.
auto schemas() -> const std::vector<type>& {
  return get_impl().schemas;
}
// Get the concepts map.
// Returns an empty map if init(...) was not called.
auto concepts() -> const concepts_map& {
  return get_impl().concepts;
}

// Get the list of modules.
[[deprecated("call modules::schemas() instead")]] auto global_module()
  -> const module* {
  if (not initialized)
    return nullptr;
  return &get_impl().mod;
}

} // namespace tenzir::modules
