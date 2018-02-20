#ifndef VAST_SYSTEM_NODE_STATE_HPP
#define VAST_SYSTEM_NODE_STATE_HPP

#include "vast/expression.hpp"
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

} // namespace system
} // namespace vast

#endif
