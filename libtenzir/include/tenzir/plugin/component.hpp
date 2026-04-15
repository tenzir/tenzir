//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/plugin/base.hpp"

#include <string>
#include <vector>

namespace tenzir {

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
