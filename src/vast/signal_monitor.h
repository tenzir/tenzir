#ifndef VAST_SIGNAL_MONITOR_H
#define VAST_SIGNAL_MONITOR_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"

namespace vast {

/// Monitors the application for UNIX signals and forwards them to an actor.
class signal_monitor : public actor_base
{
public:
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the signals.
  signal_monitor(cppa::actor receiver);

  cppa::behavior act() final;
  char const* describe() const final;

private:
  cppa::actor receiver_;
};

} // namespace vast

#endif
