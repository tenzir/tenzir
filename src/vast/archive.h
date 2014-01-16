#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/util/range_map.h"

namespace vast {

class segment;

/// The event archive. It stores events in the form of segments.
class archive
{
public:
  /// Constructs the archive.
  /// @param directory The root directory of the archive.
  archive(path directory);

  /// Retrieves the directory of the archive.
  path const& dir() const;

  /// Initializes the archive. This involves parsing the meta data of existing
  /// segments from disk an reconstructing the internal data structures to map
  /// event IDs to segments.
  void load();

  /// Stores segment meta data.
  /// @param s The segment to record meta data from.
  /// @returns `true` on success.
  bool store(segment const& s);

  /// Retrieves the segment UUID for a given event id.
  ///
  /// @param eid The event ID.
  ///
  /// @returns A tuple with the first component holding a pointer to the
  /// segment ID corresponding to *eid* and `nullptr` if *eid* does not map to
  /// a valid segment. If the first component is valid, the remaining two
  /// represent the ID interval for the segment and are set to *<0,0>*
  /// otherwise..
  std::tuple<uuid const*, event_id, event_id> lookup(event_id eid) const;

private:
  path directory_;
  util::range_map<event_id, uuid> ranges_;
};

struct archive_actor : actor<archive_actor>
{
  archive_actor(path directory, size_t max_segments);

  void act();
  char const* description() const;

  archive archive_;
  cppa::actor_ptr segment_manager_;
};

} // namespace vast

#endif
