#ifndef VAST_ACTOR_NODE_H
#define VAST_ACTOR_NODE_H

#include "vast/filesystem.h"
#include "vast/actor/actor.h"
#include "vast/actor/key_value_store.h"
#include "vast/util/system.h"

namespace vast {

/// A container for all other actors of a VAST process.
///
/// Each node stores its meta data in a central store using the following key
/// space:
///
///   - /actors/<node>/handle/<label>
///   - /actors/<node>/type/<label>
///   - /peers/<node>/<label>
///   - /nodes/<node>/...
///   - /topology
///
struct node : default_actor
{
  /// Returns the path of the log directory relative to the base directory.
  /// @returns The directory where to write log and status messages to.
  static path const& log_path();

  /// Spawns a node.
  /// @param name The name of the node.
  /// @param dir The directory where to store persistent state.
  node(std::string const& name = util::hostname(), path const& dir = "vast");

  void on_exit();
  caf::behavior make_behavior() override;

  caf::actor accountant_;
  caf::actor store_;
  std::string name_;
  path const dir_;
};

} // namespace vast

#endif
