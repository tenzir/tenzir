#ifndef VAST_ACTOR_NODE_H
#define VAST_ACTOR_NODE_H

#include <map>
#include <string>

#include "vast/filesystem.h"
#include "vast/trial.h"
#include "vast/actor/basic_state.h"
#include "vast/actor/accountant.h"
#include "vast/actor/key_value_store.h"
#include "vast/util/system.h"

namespace vast {

/// A container for all other actors of a VAST process.
///
/// Each node stores its meta data in a key-value store.
///
/// The key space has the following structure:
///
///   - /actors/<node>/<label>/{actor, type}
///   - /peers/<node>/<node>
///   - /topology/<source>/<sink>
///
struct node  {
  struct state : basic_state {
    state(local_actor* self);

    path dir;
    std::string desc;
    accountant::type accountant;
    actor store;
  };

  /// Returns the path of the log directory relative to the base directory.
  /// @returns The directory where to write log and status messages to.
  static path const& log_path();

  /// Spawns a node.
  /// @param self The actor handle
  /// @param name The name of the node.
  /// @param dir The directory where to store persistent state.
  static behavior make(stateful_actor<state>* self, std::string const& name,
                       path const& dir);
};

} // namespace vast

#endif
