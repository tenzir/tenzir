#ifndef VAST_ACTOR_SIGNAL_MONITOR_H
#define VAST_ACTOR_SIGNAL_MONITOR_H

#include "vast/actor/atoms.h"
#include "vast/actor/basic_state.h"

namespace vast {

struct signal_monitor {
  struct state : basic_state {
    state(local_actor* self);
  };

  using type = typed_actor<reacts_to<run_atom>>;
  using behavior = type::behavior_type;
  using stateful_pointer = type::stateful_pointer<state>;

  /// Monitors the application for UNIX signals.
  /// @note There must not exist more than one instance of this actor per
  ///       process.
  /// @param self The actor handle.
  /// @param receiver The actor receiving the signals.
  static behavior make(stateful_pointer self, actor receiver);
};

} // namespace vast

#endif
