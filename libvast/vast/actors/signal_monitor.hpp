#ifndef VAST_ACTOR_SIGNAL_MONITOR_HPP
#define VAST_ACTOR_SIGNAL_MONITOR_HPP

#include "vast/actor/atoms.hpp"
#include "vast/actor/basic_state.hpp"

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
