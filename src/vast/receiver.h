#ifndef VAST_RECEIVER_H
#define VAST_RECEIVER_H

#include <deque>
#include <cppa/cppa.hpp>
#include "vast/segment.h"

namespace vast {

/// Receives segments and relays them to archive and index.
class receiver : public cppa::event_based_actor
{
public:
  /// Spawns a receiver.
  /// @param tracker The tracker actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  receiver(cppa::actor_ptr tracker, cppa::actor_ptr archive,
           cppa::actor_ptr index);

  /// Implements `event_based_actor::run`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  cppa::actor_ptr tracker_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  std::deque<segment> segments_;
};

} // namespace vast

#endif
