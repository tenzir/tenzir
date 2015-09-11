#ifndef VAST_ACTOR_IMPORTER_H
#define VAST_ACTOR_IMPORTER_H

#include <vector>

#include "vast/aliases.h"
#include "vast/event.h"
#include "vast/actor/basic_state.h"

namespace vast {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct importer {
  struct state : basic_state {
    state(event_based_actor* self);

    actor identifier;
    actor archive;
    actor index;
    actor controller;
    event_id got = 0;
    std::vector<event> batch;
  };

  /// Spawns an IMPORTER.
  /// @param self The actor handle.
  static behavior make(stateful_actor<state>* self);
};

} // namespace vast

#endif
