#ifndef VAST_ACTOR_IMPORTER_HPP
#define VAST_ACTOR_IMPORTER_HPP

#include <vector>

#include "vast/aliases.hpp"
#include "vast/event.hpp"
#include "vast/actor/archive.hpp"
#include "vast/actor/basic_state.hpp"

namespace vast {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct importer {
  struct state : basic_state {
    state(event_based_actor* self);

    actor identifier;
    archive::type archive;
    actor index;
    event_id got = 0;
    std::vector<event> batch;
  };

  /// Spawns an IMPORTER.
  /// @param self The actor handle.
  static behavior make(stateful_actor<state>* self);
};

} // namespace vast

#endif
