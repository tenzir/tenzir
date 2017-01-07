#ifndef VAST_SYSTEM_NODE_HPP
#define VAST_SYSTEM_NODE_HPP

#include <string>

#include "vast/filesystem.hpp"

#include "vast/system/consensus.hpp"
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
/// @param name The name of the node.
/// @param dir The directory where to store persistent state.
/// @param id The server ID of the consensus module.
caf::behavior node(caf::stateful_actor<node_state>* self, std::string name,
                   path dir, raft::server_id id);

} // namespace system
} // namespace vast

#endif
