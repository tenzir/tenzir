#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/util/range_map.h"
#include "vast/util/lru_cache.h"

namespace vast {

class segment;

/// The event archive. It stores events in the form of segments.
class archive
{
public:
  /// Constructs the archive.
  /// @param directory The root directory of the archive.
  /// @param capacity The number of segments to hold in memory.
  archive(path directory, size_t capacity);

  /// Retrieves the directory of the archive.
  path const& dir() const;

  /// Initializes the archive. This involves parsing the meta data of existing
  /// segments from disk an reconstructing the internal data structures to map
  /// event IDs to segments.
  void initialize();

  /// Records a given segment to disk and puts it in the cache.
  /// @param msg The segment to store.
  /// @returns `true` on success.
  bool store(caf::message msg);

  /// Retrieves a segment for a given event ID.
  /// @param eid The event ID to find the segment for.
  /// @return The segment containing the event with *id*.
  trial<caf::message> load(event_id eid);

private:
  caf::message on_miss(uuid const& id);

  path dir_;
  std::unordered_map<uuid, path> segment_files_;
  util::range_map<event_id, uuid> ranges_;
  util::lru_cache<uuid, caf::message> cache_;
};

struct archive_actor : actor_base
{
  archive_actor(path const& directory, size_t max_segments);

  caf::message_handler act() final;
  std::string describe() const final;

  archive archive_;
};

} // namespace vast

#endif
