//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/command.hpp"
#include "tenzir/component_registry.hpp"
#include "tenzir/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <filesystem>
#include <map>
#include <string>
#include <unordered_map>

namespace tenzir {

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
  constexpr static inline auto name = "node";

  /// A pointer to the NODE actor handle.
  node_actor::pointer self = {};

  // -- member types -----------------------------------------------------------

  /// Stores the base directory for persistent state.
  std::filesystem::path dir = {};

  /// The component registry.
  component_registry registry = {};

  /// Components that are still alive for lifetime-tracking.
  std::set<std::pair<caf::actor_addr, std::string>> alive_components = {};

  /// Counters for multi-instance components.
  std::unordered_map<std::string, uint64_t> label_counters = {};

  /// Startup timestamp.
  time start_time = time::clock::now();

  /// Flag to signal if the node received an exit message.
  bool tearing_down = false;

  /// Weak handles to remotely spawned and monitored exec ndoes for cleanup on
  /// node shutdown.
  std::unordered_set<caf::actor_addr> monitored_exec_nodes = {};
};

/// Spawns a node.
/// @param self The actor handle
/// @param name The unique name of the node.
/// @param dir The directory where to store persistent state.
node_actor::behavior_type node(node_actor::stateful_pointer<node_state> self,
                               std::string name, std::filesystem::path dir);

} // namespace tenzir
