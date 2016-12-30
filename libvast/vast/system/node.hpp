#ifndef VAST_SYSTEM_NODE_HPP
#define VAST_SYSTEM_NODE_HPP

#include <string>

#include "vast/data.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/key_value_store.hpp"
#include "vast/system/tracker.hpp"

namespace vast {
namespace system {

//using persistent_store = key_value_store_type<std::string, data>;

/// A container for VAST components.
struct node_state {
  path dir;
  //persistent_store store;
  tracker_type tracker;
  std::string name = "node";
};

/// Spawns a node.
/// @param self The actor handle
/// @param name The name of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(caf::stateful_actor<node_state>* self, std::string name,
                   path dir);

} // namespace system
} // namespace vast

#endif
