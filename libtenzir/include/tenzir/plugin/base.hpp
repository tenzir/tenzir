//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/plugin/inspect.hpp"

#include <caf/error.hpp>

#include <memory>
#include <string>
#include <type_traits>

namespace tenzir {

/// The plugin base class.
class plugin {
public:
  /// Destroys any runtime state that the plugin created. For example,
  /// de-register from existing components, deallocate memory.
  virtual ~plugin() noexcept = default;

  /// Satisfy the rule of five.
  plugin() noexcept = default;
  plugin(const plugin&) noexcept = default;
  auto operator=(const plugin&) noexcept -> plugin& = default;
  plugin(plugin&&) noexcept = default;
  auto operator=(plugin&&) noexcept -> plugin& = default;

  /// Initializes a plugin with its respective entries from the YAML config
  /// file, i.e., `plugin.<NAME>`.
  /// @param plugin_config The relevant subsection of the configuration.
  /// @param global_config The entire Tenzir configuration for potential access
  /// to global options.
  [[nodiscard]] virtual auto
  initialize(const record& plugin_config, const record& global_config)
    -> caf::error {
    (void)plugin_config;
    (void)global_config;
    return {};
  }

  /// Returns the unique name of the plugin.
  [[nodiscard]] virtual auto name() const -> std::string = 0;
};

/// This plugin interface can be used to serialize and deserialize classes
/// derived from `Base`. To this end, the base class provides a virtual
/// `Base::name()` function, which is matched against `plugin::name()`.
template <class Base>
class serialization_plugin : public virtual plugin {
public:
  /// @pre `x.name() == name()`
  [[nodiscard]] virtual auto serialize(serializer f, const Base& x) const
    -> bool
    = 0;

  /// @post `!x || x->name() == name()`
  virtual void deserialize(deserializer f, std::unique_ptr<Base>& x) const = 0;
};

/// Implements `serialization_plugin` for a concrete class derived from `Base`
/// by using its `inspect()` overload. Also provides a default implemenetation
/// of `plugin::name()` based on `Concrete::name()`.
template <class Base, class Concrete>
  requires std::is_base_of_v<Base, Concrete>
           and std::is_default_constructible_v<Concrete>
           and std::is_final_v<Concrete>
class inspection_plugin : public virtual serialization_plugin<Base> {
public:
  auto name() const -> std::string override {
    return Concrete{}.name();
  }

  auto serialize(serializer f, const Base& op) const -> bool override {
    TENZIR_ASSERT(op.name() == name());
    auto x = dynamic_cast<const Concrete*>(&op);
    TENZIR_ASSERT(x, fmt::format("expected {}, got {} ({}) ({} == {})",
                                 typeid(Concrete).name(), typeid(op).name(),
                                 typeid(Concrete) == typeid(op),
                                 typeid(Concrete).hash_code(),
                                 typeid(op).hash_code()));
    return std::visit(
      [&](auto& f) {
        return f.get().apply(*x);
      },
      f);
  }

  void deserialize(deserializer f, std::unique_ptr<Base>& x) const override {
    x = std::visit(
      [&](auto& f) -> std::unique_ptr<Concrete> {
        auto x = std::make_unique<Concrete>();
        if (not f.get().apply(*x)) {
          f.get().set_error(caf::make_error(
            ec::serialization_error, fmt::format("inspector of `{}` failed: {}",
                                                 name(), f.get().get_error())));
          return nullptr;
        }
        return x;
      },
      f);
  }
};

} // namespace tenzir
