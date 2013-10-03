#ifndef VAST_ID_TRACKER_H
#define VAST_ID_TRACKER_H

#include <fstream>
#include "vast/actor.h"

namespace vast {

/// Keeps track of the event ID space.
class id_tracker : public actor<id_tracker>
{
public:
  /// Constructs the ID tracker.
  /// @param filename The filename containing the current ID.
  id_tracker(std::string filename);

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

  void act();
  char const* description() const;

private:
  std::string filename_;
  std::ofstream file_;
  uint64_t id_ = 1;
};

} // namespace vast

#endif
