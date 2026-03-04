//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

// This header contains the component_plugin class, which is used by plugins
// that spawn NODE components. It must be included after plugin.hpp defines
// the base plugin class.

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"

#include <string>
#include <vector>

namespace tenzir {

// Forward declaration - plugin base class defined in plugin.hpp
class plugin;

// -- component plugin --------------------------------------------------------

/// A base class for plugins that spawn components in the NODE.
/// @relates plugin
class component_plugin : public virtual plugin {
public:
  /// The name for this component in the registry.
  /// Defaults to the plugin name.
  virtual auto component_name() const -> std::string;

  /// Components that should be created before the current one so initialization
  /// can succeed.
  /// Note that the *only* guarantee made is that components are able to
  /// retrieve actor handles of the wanted components from the registry.
  /// If actors send requests before returning their behaviors, there is
  /// no guarantee that these requests will arrive at the destination in
  /// the correct order.
  /// Defaults to empty list.
  virtual auto wanted_components() const -> std::vector<std::string>;

  /// Creates an actor as a component in the NODE.
  /// @param node A stateful pointer to the NODE actor.
  /// @returns The actor handle to the NODE component.
  /// @note This function runs in the actor context of the NODE actor and can
  /// safely access the NODE's state.
  virtual auto
  make_component(node_actor::stateful_pointer<node_state> node) const
    -> component_plugin_actor
    = 0;
};

} // namespace tenzir
