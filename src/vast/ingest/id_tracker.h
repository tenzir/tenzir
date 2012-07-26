#ifndef VAST_INGEST_ID_TRACKER_H
#define VAST_INGEST_ID_TRACKER_H

#include <fstream>
#include <cppa/cppa.hpp>

namespace vast {
namespace ingest {

/// Keeps track of the event ID space.
class id_tracker : public cppa::sb_actor<id_tracker>
{
  friend class cppa::sb_actor<id_tracker>;

public:
  /// Constructs the ID tracker.
  /// @param id_file The filename containing the current ID.
  id_tracker(std::string const& id_file);

private:
  cppa::behavior init_state;
  std::ofstream file_;
  uint64_t id_ = 0;
};

} // namespace ingest
} // namespace vast

#endif
