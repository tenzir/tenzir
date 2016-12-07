#ifndef VAST_ACTOR_IDENTIFIER_HPP
#define VAST_ACTOR_IDENTIFIER_HPP

#include "vast/aliases.hpp"
#include "vast/time.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/atoms.hpp"

namespace vast {
namespace system {

/// Acquires event IDs from the NODE's key-value store.
struct identifier_state {
  caf::actor store;
  path dir;
  event_id id = 0;
  event_id available = 0;
  event_id batch_size = 1;
  timestamp last_replenish = clock::now();
  const char* name = "identifier";
};

using identifier_type = caf::typed_actor<
  caf::replies_to<id_atom>::with<event_id>,
  caf::replies_to<request_atom, event_id>::with<id_atom, event_id, event_id>
>;

/// Spawns the ID tracker.
/// @param self The actor handle.
/// @param store The key-value store to ask for more IDs.
/// @param dir The directory where to save local state to.
/// @param initial_batch_size The batch-size to start at.
identifier_type::behavior_type
identifier(identifier_type::stateful_pointer<identifier_state> self,
           caf::actor store, path dir, event_id initial_batch_size = 128);

} // namespace system
} // namespace vast

#endif
