#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// Receives chunks from IMPORTER, imbues them with an ID from TRACKER, and
/// relays them to ARCHIVE and INDEX.
struct receiver : flow_controlled_actor
{
  receiver();
  void on_exit();
  caf::behavior make_behavior() override;

  caf::actor identifier_;
  caf::actor archive_;
  caf::actor index_;
};

} // namespace vast

#endif
