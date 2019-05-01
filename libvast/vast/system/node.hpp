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

#include <map>
#include <string>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/command.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/system/tracker.hpp"

namespace vast::system {

struct node_state;

using node_actor = caf::stateful_actor<node_state>;

/// State of the node actor.
struct node_state {
  // -- member types ----------------------------------------------------------

  /// Spawns a component (actor) for the NODE with given spawn arguments.
  using component_factory = maybe_actor (*)(node_actor*, spawn_arguments&);

  /// Maps command names to component factories.
  using named_component_factories = std::map<std::string, component_factory>;

  // -- static member functions ------------------------------------------------

  static caf::message spawn_command(const command& cmd, caf::actor_system& sys,
                                    caf::settings& options,
                                    command::argument_iterator first,
                                    command::argument_iterator last);

  // -- constructors, destructors, and assignment operators --------------------

  node_state(caf::event_based_actor* selfptr);

  ~node_state();

  void init(std::string init_name, path init_dir);

  // -- member variables -------------------------------------------------------

  /// Stores the base directory for persistent state.
  path dir;

  /// Points to the instance of the tracker actor.
  tracker_type tracker;

  /// Stores how many components per label are active.
  std::map<std::string, size_t> labels;

  /// Gives the actor a recognizable name in log files.
  std::string name = "node";

  /// Points to the node itself.
  caf::event_based_actor* self;

  /// Dispatches remote commands.
  static command cmd;

  /// Maps command names (including parent command) to spawn functions.
  static named_component_factories factories;
};

/// Spawns a node.
/// @param self The actor handle
/// @param id The unique ID of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(node_actor* self, std::string id, path dir);

} // namespace vast::system

