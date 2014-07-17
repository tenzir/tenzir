#ifndef VAST_SIGNAL_MONITOR_H
#define VAST_SIGNAL_MONITOR_H

#include <caf/all.hpp>
#include "vast/actor.h"

namespace vast {

/// Monitors the application for UNIX signals and forwards them to an actor.
class signal_monitor : public actor_base
{
public:
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the signals.
  signal_monitor(caf::actor receiver);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  caf::actor receiver_;
};

} // namespace vast

#endif
