#ifndef VAST_ACTOR_IDENTIFIER_H
#define VAST_ACTOR_IDENTIFIER_H

#include "vast/aliases.h"
#include "vast/time.h"
#include "vast/filesystem.h"
#include "vast/actor/basic_state.h"

namespace vast {
namespace identifier {

struct state : basic_state {
  state(event_based_actor* self);
  ~state();

  bool flush();

  actor store;
  path dir;
  event_id id = 0;
  event_id available = 0;
  event_id batch_size = 1;
  time::moment last_replenish = time::snapshot();
};

/// Spawns the ID tracker.
/// @param self The actor handle.
/// @param store The key-value store to ask for more IDs.
/// @param dir The directory where to save local state to.
/// @param batch_size The batch-size to start at.
behavior actor(stateful_actor<state>* self, actor store,
               path dir, event_id batch_size = 128);

} // namespace identifier
} // namespace vast

#endif
