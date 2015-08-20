#ifndef VAST_ACTOR_SIGNAL_MONITOR_H
#define VAST_ACTOR_SIGNAL_MONITOR_H

#include <caf/all.hpp>

#include "vast/actor/actor.h"

namespace vast {

/// Monitors the application for UNIX signals and forwards them to an actor.
struct signal_monitor : default_actor {
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the signals.
  signal_monitor(actor receiver);

  void on_exit() override;
  behavior make_behavior() override;

  actor sink_;
};

} // namespace vast

#endif
