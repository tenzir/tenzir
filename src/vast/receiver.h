#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <queue>
#include <set>
#include "vast/actor.h"
#include "vast/segment.h"

namespace vast {

/// Receives segments, imbues them with an ID from TRACKER, and relays them to
/// ARCHIVE and INDEX. It also forwards the segment schema to SEARCH.
/// The receiver forms the server-side counterpart to an ingestor.
class receiver_actor : public actor<receiver_actor>
{
public:
  /// Spawns a receiver.
  /// @param tracker The tracker actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param search The search actor.
  receiver_actor(cppa::actor_ptr tracker,
                 cppa::actor_ptr archive,
                 cppa::actor_ptr index,
                 cppa::actor_ptr search);

  void act();
  char const* description() const;

private:
  cppa::actor_ptr tracker_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr search_;
  std::queue<segment> segments_;
  std::set<cppa::actor_ptr> ingestors_;
};

} // namespace vast

#endif
