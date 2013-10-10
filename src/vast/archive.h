#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/uuid.h"
#include "vast/util/range_map.h"

namespace vast {

/// The event archive. It stores events in the form of segments.
class archive : public actor<archive>
{
public:
  /// Spawns the archive.
  /// @param directory The root directory of the archive.
  /// @param max_segments The maximum number of segments to keep in memory.
  archive(std::string const& directory, size_t max_segments);

  void act();
  char const* description() const;

private:
  std::string directory_;
  cppa::actor_ptr segment_manager_;
  util::range_map<event_id, uuid> ranges_;
};

} // namespace vast

#endif
