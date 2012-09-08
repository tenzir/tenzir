#ifndef VAST_SOURCE_BROCCOLI_H
#define VAST_SOURCE_BROCCOLI_H

#include <string>
#include <set>
#include "vast/event_source.h"

namespace vast {
namespace source {

// TODO: Either make this a synchronous sink and inherit from
// vast::event_source or provide a separate async_source class that this
// source can inherit from.

/// Receives events from the external world.
struct broccoli : cppa::sb_actor<broccoli>
{
  /// Spawns a Broccoli event source.
  /// @param ingestor The ingestor actor.
  /// @param tracker The event ID tracker.
  broccoli(cppa::actor_ptr ingestor, cppa::actor_ptr tracker);

  cppa::actor_ptr server_;
  std::set<std::string> event_names_;
  std::set<cppa::actor_ptr> broccolis_;
  cppa::behavior init_state;
};

} // namespace source
} // namespace vast

#endif
