//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/heterogeneous_string_hash.hpp"
#include "vast/detail/tuple_map.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/expected.hpp>
#include <caf/optional.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace vast {

/// Tracks all registered components.
class component_registry {
public:
  struct component {
    caf::actor actor;
    std::string type;
  };

  /// Maps labels to components.
  using component_map = detail::heterogeneous_string_hashmap<component>;

  /// Adds a component to the registry.
  /// @param compThe component actor.
  /// @param type The type of *comp*.
  /// @param label The unique label of *comp*
  /// @returns `true` if *comp* was added successfully and `false` if
  ///          an actor for *label* exists already.
  /// @pre `comp && !type.empty()`
  [[nodiscard]] bool
  add(caf::actor comp, std::string type, std::string label = {});

  /// Removes a component from the registry.
  /// @param label The label of the component.
  /// @returns The deleted component or an error if *label* does not identify
  /// an existing component.
  [[nodiscard]] caf::expected<component> remove(std::string_view label);

  /// Removes a component from the registry.
  /// @param comp The component to erase.
  /// @returns The deleted component or an error if *comp* is not an existing
  /// actor.
  [[nodiscard]] caf::expected<component> remove(const caf::actor& comp);

  /// Finds the label of a given component actor.
  /// @param comp The component actor.
  /// @returns A pointer to the name of the label of *comp* or `nullptr` if
  ///          *comp* is not known.
  const std::string* find_label_for(const caf::actor& comp) const;

  /// Finds the type of a given component actor.
  /// @param comp The component actor.
  /// @returns A pointer to the name of the type of *comp* or `nullptr` if
  ///          *comp* is not known.
  const std::string* find_type_for(const caf::actor& comp) const;

  /// Locates a component by label.
  /// @param label The label of the component to lookup.
  /// @returns The respective component actor if found.
  caf::actor find_by_label(std::string_view label) const;

  /// Locates multiple components by label.
  /// @param l0 The label of the first component.
  /// @param l1 The label of the second component.
  /// @param ls The labels of the remaining component.
  /// @returns An array of components.
  template <class... Ts>
  std::array<caf::actor, sizeof...(Ts) + 2>
  find_by_label(std::string_view l0, std::string_view l1, Ts&&... ls) {
    return {find_by_label(l0), find_by_label(l1), find_by_label(ls)...};
  }

  /// Locates typed components by handle.
  /// @tparam Handles... The typed actor handles to lookup.
  /// @returns The respective component actors if found.
  template <class... Handles>
  std::tuple<Handles...> find() const {
    auto normalize = [](std::string in) {
      // Remove the uninteresting parts of the name:
      //   vast::test_registry_actor -> test_registry
      in.erase(0, sizeof("vast::") - 1);
      in.erase(in.size() - (sizeof("_actor") - 1));
      // Replace '_' with '-': test_registry -> test-registry
      std::replace(in.begin(), in.end(), '_', '-');
      return in;
    };
    auto labels = std::array<std::string, sizeof...(Handles)>{normalize(
      caf::type_name_by_id<caf::type_id<Handles>::value>::value.data())...};
    auto components = std::apply(
      [this](auto&&... labels) -> std::array<caf::actor, sizeof...(Handles)> {
        auto find_component = [this](auto&& label) -> caf::actor {
          if (auto i = components_.find(std::forward<decltype(label)>(label));
              i != components_.end())
            return i->second.actor;
          return {};
        };
        return {find_component(std::forward<decltype(labels)>(labels))...};
      },
      std::move(labels));
    return detail::tuple_map<std::tuple<Handles...>>(
      std::move(components), []<typename Out>(auto&& in) {
        return caf::actor_cast<Out>(std::forward<decltype(in)>(in));
      });
  }

  /// Finds all components for a given type.
  /// @param type The component type.
  /// @returns A vector of all components of the given *type* or the empty
  ///          vector if no component for *type* exists.
  std::vector<caf::actor> find_by_type(std::string_view type) const;

  /// Retrieves all compontents.
  /// @returns A reference to the internal component map.
  const component_map& components() const;

  /// Removes all entries from the registry.
  void clear() noexcept;

  /// @relates registry
  template <class Inspector>
  friend auto inspect(Inspector& f, component_registry& x) {
    return f.object(x)
      .pretty_name("component_registry")
      .fields(f.field("components", x.components_));
  }

private:
  component_map components_;
};

} // namespace vast
