#ifndef VAST_ACTOR_IDENTIFIER_HPP
#define VAST_ACTOR_IDENTIFIER_HPP

#include "vast/aliases.hpp"
#include "vast/time.hpp"
#include "vast/filesystem.hpp"
#include "vast/actor/atoms.hpp"
#include "vast/actor/basic_state.hpp"

namespace vast {

/// Acquires event IDs from the NODE's key-value store.
struct identifier { 
  struct state : basic_state {
    state(local_actor* self);
    ~state();

    bool flush();

    actor store;
    path dir;
    event_id id = 0;
    event_id available = 0;
    event_id batch_size = 1;
    time::moment last_replenish = time::snapshot();
  };

  using type =
    typed_actor<
      replies_to<id_atom>::with<event_id>,
      replies_to<request_atom, event_id>
        ::with_either<id_atom, event_id, event_id>
        ::or_else<error>
    >;
  using behavior = type::behavior_type;
  using stateful_pointer = type::stateful_pointer<state>;

  /// Spawns the ID tracker.
  /// @param self The actor handle.
  /// @param store The key-value store to ask for more IDs.
  /// @param dir The directory where to save local state to.
  /// @param batch_size The batch-size to start at.
  static behavior make(stateful_pointer self, actor store,
                       path dir, event_id batch_size = 128);
};

} // namespace vast

#endif
