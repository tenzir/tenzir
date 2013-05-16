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
  /// @param file_name The filename containing the current ID.
  id_tracker(std::string file_name);

  /// Overrides `cppa::event_based_actor::init`.
  virtual void init() final;

  /// Overrides `cppa::event_based_actor::init`.
  virtual void on_exit() final;

private:
  std::string file_name_;
  std::ofstream file_;
  uint64_t id_ = 0;
};

} // namespace vast

#endif
