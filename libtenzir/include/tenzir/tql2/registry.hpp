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
#include "tenzir/type.hpp"

#include <memory>
#include <shared_mutex>

namespace tenzir {

struct module_def;

/// Operators defined in the user's config.
struct user_defined_operator {
  /// Definition with resolved entities, but without resolved `let`s.
  ast::pipeline definition;

  /// Parameter definitions for arguments and options
  struct parameter {
    std::string name;
    std::string type_hint;
    std::optional<std::string> description;
    std::optional<ast::expression> default_value;
    std::optional<type> value_type;
  };
  std::vector<parameter> positional_params;
  std::vector<parameter> named_params;
};

class operator_compiler_plugin;

/// Operators defined natively with C++.
struct native_operator {
  native_operator(const operator_compiler_plugin* ir_plugin,
                  const operator_factory_plugin* factory_plugin)
    : ir_plugin{ir_plugin}, factory_plugin{factory_plugin} {
  }

  // We have at least one of these set, maybe both.
  const operator_compiler_plugin* ir_plugin;
  const operator_factory_plugin* factory_plugin;
};

/// Operators are either native or user-defined.
struct operator_def {
public:
  explicit(false) operator_def(user_defined_operator udo)
    : kind_{std::move(udo)} {
  }

  explicit(false) operator_def(native_operator builtin) : kind_{builtin} {
  }

  explicit(false) operator_def(const operator_factory_plugin& plugin)
    : kind_{native_operator{nullptr, &plugin}} {
  }

  /// Instantiate the operator with the given arguments.
  auto make(operator_factory_plugin::invocation inv, session ctx) const
    -> failure_or<operator_ptr>;

  // TODO: Remove this?
  auto inner() const -> const variant<native_operator, user_defined_operator>& {
    return kind_;
  }
  auto inner() -> variant<native_operator, user_defined_operator>& {
    return kind_;
  }

private:
  variant<native_operator, user_defined_operator> kind_;
};

/// A set of entities, with a most one entity per entity namespace.
struct entity_set {
  const function_plugin* fn;
  std::optional<operator_def> op;
  std::unique_ptr<module_def> mod;
};

/// A module is a collection of named entities.
struct module_def {
  module_def() = default;
  ~module_def() = default;
  module_def(const module_def&) = delete;
  module_def(module_def&&) noexcept = default;
  auto operator=(const module_def&) -> module_def& = delete;
  auto operator=(module_def&&) noexcept -> module_def& = default;

  std::unordered_map<std::string, entity_set> defs;
};

/// The definition of an entity. Modules are not included here because they are
/// currently only created implicitly by other entities.
using entity_def
  = variant<operator_def, std::reference_wrapper<const function_plugin>>;

/// Reference to any entity, including modules.
using entity_ref = variant<std::reference_wrapper<const function_plugin>,
                           std::reference_wrapper<const operator_def>,
                           std::reference_wrapper<const module_def>>;

/// The registry holds references to all known entities and can thus be used to
/// resolve an `entity_path` to an `entity_ref`.
class registry {
public:
  struct error {
    /// The index of the segment that we could not resolve.
    size_t segment{};
    /// Whether there exists an entity of a different namespace for that segment.
    bool other_exists{};
  };

  /// Try to resolve an entity path.
  auto try_get(const entity_path& path) const -> variant<entity_ref, error>;

  /// Resolve an entity path or panic if it fails.
  auto get(const ast::function_call& call) const -> const function_plugin&;
  auto get(const ast::invocation& inv) const -> const operator_def&;
  auto get(const entity_path& path) const -> entity_ref;

  /// Return a list of all entities for the given namespace.
  auto entity_names(entity_ns ns) const -> std::vector<std::string>;
  auto operator_names() const -> std::vector<std::string>;
  auto function_names() const -> std::vector<std::string>;
  auto module_names() const -> std::vector<std::string>;

  /// Register an entity. This should only be done on startup or when
  /// constructing a new snapshot of the registry.
  void add(const entity_pkg& package, std::string_view name, entity_def def);

  /// Create a deep copy of this registry. Used for snapshot-style updates.
  auto clone() const -> std::unique_ptr<registry>;

  /// Add a module definition at the given path, creating parent modules as
  /// necessary. Fails with an assertion if a module already exists there.
  void add_module(const entity_pkg& package, std::string_view path,
                  std::unique_ptr<module_def> mod);

  /// Replace (or create) a module definition at the given path, creating
  /// parent modules as necessary.
  void replace_module(const entity_pkg& package, std::string_view path,
                      std::unique_ptr<module_def> mod);

  /// Remove the module at the given path. Parent modules are left intact. If
  /// the name entry becomes empty (no fn/op/mod), it is erased.
  void remove_module(const entity_pkg& package, std::string_view path);

private:
  /// Get the root module for the given package.
  auto root(const entity_pkg& package) -> module_def&;
  auto root(const entity_pkg& package) const -> const module_def&;
  std::unordered_map<std::string, module_def, detail::heterogeneous_string_hash,
                     std::equal_to<>>
    roots_;
};

// TODO: This should be attached to the `session` object. However, because we
// are still in the process of upgrading everything, we cannot consistently pass
// this around.
/// Return the current global registry snapshot.
auto global_registry() -> std::shared_ptr<const registry>;

// Note: Mutable access to a global registry is intentionally not exposed
// anymore; use clone() and publish via a swap in higher-level control flows.

/// RAII guard to serialize a full clone->update->publish cycle.
class registry_update_guard {
public:
  registry_update_guard(const registry_update_guard&) = delete;
  auto operator=(const registry_update_guard&)
    -> registry_update_guard& = delete;
  registry_update_guard(registry_update_guard&&) noexcept = default;
  auto operator=(registry_update_guard&&) noexcept
    -> registry_update_guard& = default;
  ~registry_update_guard() = default;

  /// Return the current global registry snapshot while holding the lock.
  auto current() const -> std::shared_ptr<const registry>;

  /// Publish a new global registry snapshot while holding the lock.
  void publish(std::shared_ptr<const registry>&& next) const;

private:
  friend auto begin_registry_update() -> registry_update_guard;
  explicit registry_update_guard(std::unique_lock<std::shared_mutex> lock)
    : lock_{std::move(lock)} {
  }

  std::unique_lock<std::shared_mutex> lock_;
};

/// Acquire the registry update lock to perform clone->update->publish atomically.
auto begin_registry_update() -> registry_update_guard;

} // namespace tenzir
