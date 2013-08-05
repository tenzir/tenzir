#ifndef VAST_SYSTEM_MONITOR_H
#define VAST_SYSTEM_MONITOR_H

#include <cppa/cppa.hpp>
#include "vast/configuration.h"

namespace vast {

/// Monitors the application for system events (such as keystrokes and signals).
/// and forwards them to a given actor.
class system_monitor : public cppa::event_based_actor
{
public:
  /// Spawns the system monitor with a given receiver.
  /// @param receiver the actor receiving the keystrokes.
  system_monitor(cppa::actor_ptr receiver);

  /// Overrides `cppa::event_based_actor::init`.
  void init() final;

  /// Overrides `cppa::event_based_actor::on_exit`.
  void on_exit() final;

private:
  void stop();

  cppa::actor_ptr upstream_;
};

} // namespace vast

#endif
