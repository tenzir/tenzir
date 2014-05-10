#ifndef VAST_SEGMENT_MANAGER_H
#define VAST_SEGMENT_MANAGER_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/cow.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/util/lru_cache.h"

namespace vast {

class segment;

/// Manages the segments on disk an in-memory segments in a LRU fashion.
class segment_manager
{
public:
  /// Constructs a segment manager.
  ///
  /// @param capacity The number of segments to keep in memory until old ones
  /// should be evicted.
  ///
  /// @param dir The directory with the segments.
  segment_manager(size_t capacity, path dir);

  /// Records a given segment to disk and puts it in the cache.
  /// @param cs The segment to store.
  /// @returns `true` on success.
  bool store(cow<segment> const& cs);

  /// Retrieves a segment.
  /// @param id The ID of the segment to retrieve.
  /// @return The segment with ID *id*.
  cow<segment> lookup(uuid const& id);

private:
  cow<segment> on_miss(uuid const& id);

  path const dir_;
  util::lru_cache<uuid, cow<segment>> cache_;
  std::unordered_map<uuid, path> segment_files_;
};

struct segment_manager_actor : actor_base
{
  segment_manager_actor(size_t capacity, path dir);

  cppa::behavior act() final;
  char const* describe() const final;

  segment_manager segment_manager_;
};

} // namespace vast

#endif
