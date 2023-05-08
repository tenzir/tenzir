//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/aliases.hpp"
#include "vast/command.hpp"
#include "vast/error.hpp"
#include "vast/plugin.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/component_registry.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <filesystem>
#include <map>
#include <string>
#include <unordered_map>

namespace vast::system {

enum class detach_components {
  yes,
  no,
};

/// State of the node actor.
struct node_state {
  // -- remote command infrastructure ------------------------------------------

  /// Command callback for spawning components in the node.
  static caf::message
  spawn_command(const invocation& inv, caf::actor_system& sys);

  /// Spawns a component for the NODE with given spawn arguments.
  using component_factory_fun
    = caf::expected<caf::actor> (*)(node_actor::stateful_pointer<node_state>,
                                    spawn_arguments&);

  /// Maps command names to a component factory.
  using named_component_factory = std::map<std::string, component_factory_fun>;

  /// Maps command names (including parent command) to spawn functions.
  inline static named_component_factory component_factory = {};

  /// Maps command names to functions.
  inline static command::factory command_factory = {};

  // -- rest handling infrastructure -------------------------------------------

  using handler_and_endpoint = std::pair<rest_handler_actor, rest_endpoint>;

  /// Retrieve or spawn the handler actor for the given request.
  auto get_endpoint_handler(const http_request_description& desc)
    -> const handler_and_endpoint&;

  /// The REST endpoint handlers for this node. Spawned on demand.
  std::unordered_map<std::string, handler_and_endpoint> rest_handlers = {};

  // -- actor facade -----------------------------------------------------------

  /// The name of the NODE actor.
  std::string name = "node";

  /// A pointer to the NODE actor handle.
  node_actor::pointer self = {};

  // -- member types -----------------------------------------------------------

  /// Stores the base directory for persistent state.
  std::filesystem::path dir = {};

  /// The component registry.
  component_registry registry = {};

  /// Counters for multi-instance components.
  std::unordered_map<std::string, uint64_t> label_counters = {};

  /// Flag to signal if the node received an exit message.
  bool tearing_down = false;
};

/// Spawns a node.
/// @param self The actor handle
/// @param name The unique name of the node.
/// @param detach_components Whether to spawn some components (ie. the
/// filesystem) in
///        dedicated threads.
/// @param dir The directory where to store persistent state.
node_actor::behavior_type
node(node_actor::stateful_pointer<node_state> self, std::string name,
     std::filesystem::path dir, detach_components);

} // namespace vast::system
