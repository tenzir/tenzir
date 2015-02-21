#ifndef VAST_ACTOR_EXPORTER_H
#define VAST_ACTOR_EXPORTER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// An actor receiving events and dispatching them to registered sinks.
struct exporter : default_actor
{
  exporter();
  void on_exit();
  caf::behavior make_behavior() override;

  std::set<caf::actor> sinks_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace vast

#endif
