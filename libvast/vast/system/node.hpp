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

  /// Identifies a command and its parent by name. The two names in conjunction
  /// uniquely identify a component, e.g., by disambiguating "import pcap" from
  /// "export pcap".
  using atom_pair = std::pair<caf::atom_value, caf::atom_value>;

  /// Maps command names to component factories.
  using named_component_factories = std::map<atom_pair, component_factory>;

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
  std::unordered_map<std::string, size_t> labels;

  /// Gives the actor a recognizable name in log files.
  std::string name = "node";

  /// Dispatches remote commands.
  command cmd;

  /// Points to the node itself.
  caf::event_based_actor* self;

  /// Maps command names (including parent command) to spawn functions.
  static named_component_factories factories;
};

/// Spawns a node.
/// @param self The actor handle
/// @param id The unique ID of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(node_actor* self, std::string id, path dir);

} // namespace vast::system

