/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/fwd.hpp"

#include <caf/actor.hpp>
#include <caf/actor_cast.hpp>
#include <caf/meta/type_name.hpp>
#include <caf/optional.hpp>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace vast::system {

/// Tracks all registered components.
class component_registry {
public:
  struct component {
    caf::actor actor;
    std::string type;
  };

  /// Maps labels to components.
  using component_map = std::unordered_map<std::string, component>;

  /// Adds a component to the registry.
  /// @param compThe component actor.
  /// @param type The type of *comp*.
  /// @param label The unique label of *comp*
  /// @returns `true` if *comp* was added successfully and `false` if
  ///          an actor for *label* exists already.
  bool add(caf::actor comp, std::string type, std::string label = {});

  /// Removes a component from the registry.
  /// @param comp The component to remove
  /// @returns `true` iff the component was deleted successfully.
  bool remove(const caf::actor& comp);

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

  /// Locates multiple components by actor type.
  /// @tparam Handle the actor type.
  /// @returns An array of components.
  template <class... Handles>
  std::tuple<Handles...> find() {
    return std::tuple<Handles...>{
      caf::actor_cast<Handles>(find_by_label(Handles::name))...};
  }

  /// Finds all components for a given type.
  /// @param type The component type.
  /// @returns A vector of all components of the given *type* or the empty
  ///          vector if no component for *type* exists.
  std::vector<caf::actor> find_by_type(std::string_view type) const;

  /// Retrieves all compontents.
  /// @returns A reference to the internal component map.
  const component_map& components() const;

  /// @relates registry
  template <class Inspector>
  friend auto inspect(Inspector& f, component_registry& x) {
    return f(caf::meta::type_name("component_registry"), x.components_);
  }

private:
  component_map components_;
};

} // namespace vast::system
