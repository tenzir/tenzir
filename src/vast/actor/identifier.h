#ifndef VAST_ACTOR_IDENTIFIER_H
#define VAST_ACTOR_IDENTIFIER_H

#include "vast/actor/actor.h"
#include "vast/aliases.h"
#include "vast/filesystem.h"

namespace vast {

/// Keeps track of the event ID space.
struct identifier : default_actor {
  /// Constructs the ID tracker.
  /// @param dir The directory where to save the ID to.
  identifier(path dir);

  caf::behavior make_behavior() override;

  bool save_id();

  path dir_;
  event_id id_ = 0;
};

} // namespace vast

#endif
