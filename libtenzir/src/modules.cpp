//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/modules.hpp"

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/taxonomies.hpp"

#include <boost/unordered/unordered_flat_map.hpp>

#include <mutex>
#include <utility>

namespace tenzir::modules {

namespace {

struct global_module_registry {
  std::mutex mutex;
  boost::unordered_flat_map<std::string, variant<type, legacy_type>,
                            detail::heterogeneous_string_hash,
                            detail::heterogeneous_string_equal>
    types;

  concepts_map concepts;
};

bool initialized = false;

auto get_impl() -> global_module_registry& {
  static global_module_registry data;
  return data;
}

} // namespace

void init(symbol_map symbols, concepts_map concepts) {
  TENZIR_ASSERT(not initialized);
  auto& impl = get_impl();
  auto lock = std::unique_lock{impl.mutex};
  for (auto& [name, ty] : symbols) {
    TENZIR_ASSERT(name == ty.name());
    if (is<legacy_record_type>(ty)) {
      impl.types.emplace(name, std::move(ty));
    }
  }
  get_impl().concepts = std::move(concepts);
  initialized = true;
}

auto get_schema(std::string_view name) -> std::optional<type> {
  auto& global = get_impl();
  // The critical section here is very small once the type has been converted.
  // This function should thus be fine to call outside of tight loops.
  auto lock = std::unique_lock{global.mutex};
  auto it = global.types.find(name);
  if (it == global.types.end()) {
    return std::nullopt;
  }
  return match(
    it->second,
    [](const type& ty) {
      return ty;
    },
    [&](const legacy_type& legacy) {
      auto converted = type::from_legacy_type(legacy);
      it->second = converted;
      return converted;
    });
}

// Get the concepts map.
// Returns an empty map if init(...) was not called.
auto concepts() -> const concepts_map& {
  return get_impl().concepts;
}

} // namespace tenzir::modules
