#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <set>
#include "vast/actor.h"

namespace vast {

/// Receives chunks from IMPORTER, imbues them with an ID from TRACKER, and
/// relays them to ARCHIVE and INDEX. RECEIVER also forwards the chunk schema
/// to SEARCH.
class receiver : public actor_base
{
public:
  /// Spawns a receiver.
  /// @param tracker The tracker actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param search The search actor.
  receiver(caf::actor tracker,
           caf::actor archive,
           caf::actor index,
           caf::actor search);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  bool paused_ = false;
  caf::actor tracker_;
  caf::actor archive_;
  caf::actor index_;
  caf::actor search_;
  std::set<caf::actor> importers_;
};

} // namespace vast

#endif
