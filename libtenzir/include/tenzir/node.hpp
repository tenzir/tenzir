//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/component_registry.hpp"
#include "tenzir/endpoint.hpp"
#include "tenzir/series_builder.hpp"

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <chrono>
#include <deque>
#include <filesystem>
#include <string>
#include <unordered_map>

#include <reproc++/reproc.hpp>

namespace tenzir {

/// State of the node actor.
struct node_state {
  // -- rest handling infrastructure -------------------------------------------

  using handler_and_endpoint = std::pair<rest_handler_actor, rest_endpoint>;

  /// Retrieve or spawn the handler actor for the given request.
  auto get_endpoint_handler(const http_request_description& desc)
    -> const handler_and_endpoint&;

  /// The REST endpoint handlers for this node. Spawned on demand.
  std::unordered_map<std::string, handler_and_endpoint> rest_handlers;

  // -- actor facade -----------------------------------------------------------

  /// The name of the NODE actor.
  constexpr static auto name = "node";

  /// A pointer to the NODE actor handle.
  node_actor::pointer self = {};

  // -- member functions -------------------------------------------------------

  auto create_pipeline_shell() -> void;

  auto get_pipeline_shell() -> caf::result<pipeline_shell_actor>;

  auto connect_pipeline_shell(uint32_t child_id, pipeline_shell_actor handle)
    -> caf::result<void>;

  auto monitor_shell_for_pipe(caf::strong_actor_ptr client,
                              reproc::process proc) -> void;

  // -- member types -----------------------------------------------------------

  /// Stores the base directory for persistent state.
  std::filesystem::path dir;

  /// The component registry.
  component_registry registry = {};

  /// The list of component plugin actors in the order that they were spawned.
  std::vector<std::string> ordered_components;

  /// Components that are still alive for lifetime-tracking.
  std::set<std::pair<caf::actor_addr, std::string>> alive_components;

  /// Map from component actor address to name for better error messages. Never
  /// cleared.
  std::unordered_map<caf::actor_addr, std::string> component_names;

  /// Counters for multi-instance components.
  std::unordered_map<std::string, uint64_t> label_counters;

  /// Builder for API metrics.
  std::unordered_map<std::string, series_builder> api_metrics_builders;

  /// Startup timestamp.
  time start_time = time::clock::now();

  /// Flag to signal if the node received an exit message.
  bool tearing_down = false;

  /// Listening endpoint.
  std::optional<tenzir::endpoint> endpoint;

  /// Weak handles to remotely spawned and monitored exec ndoes for cleanup on
  /// node shutdown.
  std::unordered_set<caf::actor_addr> monitored_exec_nodes;

  /// Whether to create pipeline shells.
  bool pipeline_subprocesses = false;

  /// Response promises for pending subprocess creations.
  std::deque<caf::typed_response_promise<pipeline_shell_actor>>
    shell_response_promises;

  /// Initializing pipeline shell child processes.
  std::unordered_map<uint32_t, reproc::process> creating_pipeline_shells;

  /// Counter for creating child processes. Used to identify created processes
  /// in the connection handler.
  uint32_t child_id = 0;

  /// Pool of pre-created pipeline shell child processes.
  struct pipeline_subprocess {
    reproc::process process;
    pipeline_shell_actor handle;
  };
  std::deque<pipeline_subprocess> created_pipeline_shells;

  /// Pipeline shells that are currently allocated to a pipeline_executor.
  std::unordered_map<caf::actor_addr, reproc::process> owned_shells;
};

/// Spawns a node.
/// @param self The actor handle
/// @param dir The directory where to store persistent state.
/// @param pipeline_subprocesses Whether to enable pipeline subprocesses.
node_actor::behavior_type
node(node_actor::stateful_pointer<node_state> self, std::filesystem::path dir,
     bool pipeline_subprocesses);

} // namespace tenzir
