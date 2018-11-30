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

#include <string>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/command.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/tracker.hpp"

namespace vast::system {

/// State of the node actor.
struct node_state {
  // -- constructors, destructors, and assignment operators --------------------

  node_state(caf::event_based_actor* selfptr);

  ~node_state();

  void init(std::string init_name, path init_dir);

  // -- member variables -------------------------------------------------------

  /// Stores the base directory for persistent state.
  path dir;

  /// Points to the instance of the tracker actor.
  tracker_type tracker;

  /// Points to the instance of the accountant actor.
  accountant_type accountant;

  /// Stores how many components per label are active.
  std::unordered_map<std::string, size_t> labels;

  /// Gives the actor a recognizable name in log files.
  std::string name = "node";

  /// Dispatches remote commands.
  command cmd;

  /// Points to the node itself.
  caf::event_based_actor* self;
};

using node_actor = caf::stateful_actor<node_state>;

/// Spawns a node.
/// @param self The actor handle
/// @param id The unique ID of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(node_actor* self, std::string id, path dir);

} // namespace vast::system

