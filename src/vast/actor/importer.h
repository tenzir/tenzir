#ifndef VAST_ACTOR_IMPORTER_H
#define VAST_ACTOR_IMPORTER_H

#include <set>
#include <vector>

#include "vast/aliases.h"
#include "vast/event.h"
#include "vast/actor/actor.h"

namespace vast {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct importer : flow_controlled_actor {
  importer();

  void on_exit() override;
  caf::behavior make_behavior() override;

  caf::actor identifier_;
  caf::actor archive_;
  caf::actor index_;
  event_id got_ = 0;
  std::vector<event> batch_;
};

} // namespace vast

#endif
