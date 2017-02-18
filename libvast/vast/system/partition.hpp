#ifndef VAST_SYSTEM_PARTITION_HPP
#define VAST_SYSTEM_PARTITION_HPP

#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/filesystem.hpp"
#include "vast/type.hpp"

namespace vast {
namespace system {

struct partition_state {
  std::unordered_map<type, caf::actor> indexers;
  const char* name = "partition";
};

/// A horizontal partition of the INDEX.
/// For each event batch, PARTITION spawns one event indexer per
/// type occurring in the batch and forwards to them the events.
/// @param dir The directory where to store this partition on the file system.
caf::behavior partition(caf::stateful_actor<partition_state>* self, path dir);

} // namespace system
} // namespace vast

#endif
