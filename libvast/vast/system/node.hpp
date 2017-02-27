#ifndef VAST_SYSTEM_NODE_HPP
#define VAST_SYSTEM_NODE_HPP

#include <string>

#include "vast/filesystem.hpp"

#include "vast/system/tracker.hpp"

namespace vast {
namespace system {

/// A container for VAST components.
struct node_state {
  path dir;
  tracker_type tracker;
  std::unordered_map<std::string, int> labels;
  std::string name = "node";
};

/// Spawns a node.
/// @param self The actor handle
/// @param id The unique ID of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(caf::stateful_actor<node_state>* self, std::string id,
                   path dir);

} // namespace system
} // namespace vast

#endif
