#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <set>
#include "vast/actor.h"
#include "vast/segment.h"

namespace vast {

/// Receives segments, imbues them with an ID from TRACKER, and relays them to
/// ARCHIVE and INDEX. It also forwards the segment schema to SEARCH.
/// The receiver forms the server-side counterpart to an ingestor.
class receiver_actor : public actor_base
{
public:
  /// Spawns a receiver.
  /// @param tracker The tracker actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param search The search actor.
  receiver_actor(cppa::actor tracker,
                 cppa::actor archive,
                 cppa::actor index,
                 cppa::actor search);

  cppa::behavior act() final;
  std::string describe() const final;

private:
  bool paused_ = false;
  cppa::actor tracker_;
  cppa::actor archive_;
  cppa::actor index_;
  cppa::actor search_;
  std::set<cppa::actor> ingestors_;
};

} // namespace vast

#endif
