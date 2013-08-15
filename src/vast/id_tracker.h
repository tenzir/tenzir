#ifndef VAST_ID_TRACKER_H
#define VAST_ID_TRACKER_H

#include <fstream>
#include <cppa/cppa.hpp>

namespace vast {

/// Keeps track of the event ID space.
class id_tracker : public cppa::event_based_actor
{
public:
  /// Constructs the ID tracker.
  /// @param filename The filename containing the current ID.
  id_tracker(std::string filename);

  /// Implements `cppa::event_based_actor::init`.
  virtual void init() final;

  /// Overrides `cppa::event_based_actor::init`.
  virtual void on_exit() final;

private:
  std::string filename_;
  std::ofstream file_;
  uint64_t id_ = 1;
};

} // namespace vast

#endif
