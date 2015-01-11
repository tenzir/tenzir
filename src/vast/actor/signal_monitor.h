#ifndef VAST_ACTOR_SIGNAL_MONITOR_H
#define VAST_ACTOR_SIGNAL_MONITOR_H

#include <caf/all.hpp>
#include "vast/actor/actor.h"

namespace vast {

/// Monitors the application for UNIX signals and forwards them to an actor.
class signal_monitor : public actor_mixin<signal_monitor, sentinel>
{
public:
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the signals.
  signal_monitor(caf::actor receiver);

  caf::message_handler make_handler();
  std::string name() const;

private:
  caf::actor receiver_;
};

} // namespace vast

#endif
