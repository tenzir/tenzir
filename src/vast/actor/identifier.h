#ifndef VAST_ACTOR_IDENTIFIER_H
#define VAST_ACTOR_IDENTIFIER_H

#include "vast/aliases.h"
#include "vast/time.h"
#include "vast/filesystem.h"
#include "vast/actor/actor.h"

namespace vast {

/// Keeps track of the event ID space.
struct identifier : default_actor {
  /// Constructs the ID tracker.
  /// @param store The key-value store to ask for more IDs
  /// @param dir The directory where to save local state to.
  /// @param batch_size The batch-size to start at.
  identifier(caf::actor store, path dir, event_id batch_size = 128);

  void on_exit();
  caf::behavior make_behavior() override;

  bool save_state();

  caf::actor store_;
  path const dir_;
  event_id id_ = 0;
  event_id available_ = 0;
  event_id batch_size_;
  time::moment last_replenish_ = time::snapshot();
};

} // namespace vast

#endif
