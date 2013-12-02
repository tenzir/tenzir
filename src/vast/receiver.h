#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <deque>
#include "vast/actor.h"
#include "vast/segment.h"

namespace vast {

/// Receives segments, imbues them with an ID from the tracker, and relays them
/// to archive and index. The receiver forms the server-sdie counterpart to an
/// ingestor.
class receiver : public actor<receiver>
{
public:
  /// Spawns a receiver.
  /// @param tracker The tracker actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  receiver(cppa::actor_ptr tracker, cppa::actor_ptr archive,
           cppa::actor_ptr index);

  void act();
  char const* description() const;

private:
  cppa::actor_ptr tracker_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  std::deque<segment> segments_;
};

} // namespace vast

#endif
