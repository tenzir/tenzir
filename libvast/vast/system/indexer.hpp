#ifndef VAST_SYSTEM_INDEXER_HPP
#define VAST_SYSTEM_INDEXER_HPP

#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "vast/filesystem.hpp"
#include "vast/type.hpp"

namespace vast {
namespace system {

struct event_indexer_state {
  path dir;
  type event_type;
  std::unordered_map<path, caf::actor> indexers;
  const char* name = "event-indexer";
};

/// Indexes an event.
/// @param self The actor handle.
/// @param dir The directory where to store the indexes in.
/// @param type event_type The type of the event to index.
caf::behavior event_indexer(caf::stateful_actor<event_indexer_state>* self,
                            path dir, type event_type);

} // namespace system
} // namespace vast

#endif
