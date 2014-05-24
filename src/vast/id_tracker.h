#ifndef VAST_ID_TRACKER_H
#define VAST_ID_TRACKER_H

#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/file_system.h"

namespace vast {

/// Keeps track of the event ID space.
class id_tracker
{
public:
  /// Constructs the ID tracker.
  /// @param dir The directory where to save the ID to.
  id_tracker(path dir);

  bool load();
  bool save();
  event_id next_id() const;

  /// Hands out a given number of events.
  bool hand_out(uint64_t n);

private:
  path dir_;
  event_id id_ = 1;
};

/// Keeps track of the event ID space.
struct id_tracker_actor : actor_base
{
  id_tracker_actor(path dir);

  cppa::behavior act() final;
  std::string describe() const final;

  id_tracker id_tracker_;
};

} // namespace vast

#endif
