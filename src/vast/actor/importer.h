#ifndef VAST_ACTOR_IMPORTER_H
#define VAST_ACTOR_IMPORTER_H

#include <vector>

#include "vast/aliases.h"
#include "vast/event.h"
#include "vast/actor/basic_state.h"

namespace vast {
namespace importer {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct state : basic_state {
  state(event_based_actor* self);

  actor identifier;
  actor archive;
  actor index;
  actor controller;
  event_id got = 0;
  std::vector<event> batch;
};

behavior actor(stateful_actor<state>* self);

} // namespace importer
} // namespace vast

#endif
