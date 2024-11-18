//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <caf/detail/scope_guard.hpp>

namespace tenzir {

struct module_def;

/// A set of entities, with a most one entity per entity namespace.
struct entity_set {
  const function_plugin* fn;
  const operator_factory_plugin* op;
  std::unique_ptr<module_def> mod;
};

struct module_def {
  // TODO: Why are these needed?
  module_def() = default;
  module_def(const module_def&) = delete;
  module_def(module_def&&) = default;
  auto operator=(const module_def&) = delete;
  auto operator=(module_def&&) -> module_def& = default;

  std::unordered_map<std::string, entity_set> defs;
};

/// Reference to a single entity definition.
using entity_def
  = variant<std::reference_wrapper<const function_plugin>,
            std::reference_wrapper<const operator_factory_plugin>,
            std::reference_wrapper<const module_def>>;

// TODO: The interface of this class is drastically simplified for now. It
// must be changed eventually to properly enable modules and use an
// interned representation of `entity_path`.
class registry {
public:
  struct error {
    size_t segment{};
    bool other_exists{};
  };

  auto try_get(const entity_path& path) const -> variant<entity_def, error>;

  auto get(const ast::function_call& call) const -> const function_plugin&;
  auto get(const ast::invocation& call) const -> const operator_factory_plugin&;
  auto get(const entity_path& path) const -> entity_def;

  auto operator_names() const -> std::vector<std::string>;
  auto function_names() const -> std::vector<std::string>;

  /// Register an entity. This should only be done on startup.
  void add(std::string name, entity_def def);

  auto root_module() const -> const module_def&;

private:
  module_def root_;
};

// TODO: This should be attached to the `session` object. However, because we
// are still in the process of upgrading everything, we cannot consistently pass
// this around.
auto global_registry() -> const registry&;

auto thread_local_registry() -> const registry*;

void set_thread_local_registry(const registry* reg);

template <class F>
auto with_thread_local_registry(const registry& reg, F&& f) -> decltype(auto) {
  auto prev = thread_local_registry();
  set_thread_local_registry(&reg);
  auto guard = caf::detail::scope_guard{[&] {
    set_thread_local_registry(prev);
  }};
  return std::invoke(std::forward<F>(f));
}

} // namespace tenzir
