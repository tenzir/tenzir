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

#include "vast/aliases.hpp"
#include "vast/command.hpp"
#include "vast/error.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/archive.hpp"
#include "vast/system/component_registry.hpp"
#include "vast/system/spawn_arguments.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <map>
#include <string>

namespace vast::system {

struct node_state;

using node_actor = caf::stateful_actor<node_state>;

/// State of the node actor.
struct node_state {
  /// Spawns a component (actor) for the NODE with given spawn arguments.
  using component_factory_fun = maybe_actor (*)(node_actor*, spawn_arguments&);

  /// Maps command names to a component factory.
  using named_component_factory = std::map<std::string, component_factory_fun>;

  static caf::message
  spawn_command(const invocation& inv, caf::actor_system& sys);

  /// Maps command names (including parent command) to spawn functions.
  inline static named_component_factory component_factory = {};

  /// Optionally creates extra component mappings.
  inline static named_component_factory (*extra_component_factory)() = nullptr;

  /// Maps command names to functions.
  inline static command::factory command_factory = {};

  /// Optionally creates extra component mappings.
  inline static command::factory (*extra_command_factory)() = nullptr;

  /// Stores the base directory for persistent state.
  path dir;

  /// The component registry.
  component_registry registry;

  /// Counters for multi-instance components.
  std::unordered_map<std::string, uint64_t> label_counters;

  /// Gives the actor a recognizable name in log files.
  std::string name;
};

/// Spawns a node.
/// @param self The actor handle
/// @param name The unique name of the node.
/// @param dir The directory where to store persistent state.
/// @param shutdown_grace_period Time to give components to shutdown cleanly.
caf::behavior node(node_actor* self, std::string name, path dir,
                   std::chrono::milliseconds shutdown_grace_period);

} // namespace vast::system
