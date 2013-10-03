#ifndef VAST_SIGNAL_MONITOR_H
#define VAST_SIGNAL_MONITOR_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"

namespace vast {

/// Monitors the application for UNIX signals and forwards them to an actor.
class signal_monitor : public actor<signal_monitor>
{
public:
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the signals.
  signal_monitor(cppa::actor_ptr receiver);

  /// Overrides `cppa::event_based_actor::on_exit`.
  void on_exit() final;

  void act();
  char const* description() const;

private:
  cppa::actor_ptr receiver_;
};

} // namespace vast

#endif
