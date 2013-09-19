#ifndef VAST_SEGMENT_MANAGER_H
#define VAST_SEGMENT_MANAGER_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/cow.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/util/lru_cache.h"

namespace vast {

// Forward declarations.
class segment;

/// Manages the segments on disk an in-memory segments in a LRU fashion.
class segment_manager : public cppa::event_based_actor
{
public:
  /// Spawns the segment manager.
  ///
  /// @param capacity The number of segments to keep in memory until old ones
  /// should be evicted.
  ///
  /// @param dir The directory with the segments.
  segment_manager(size_t capacity, std::string const& dir);

  /// Implements `event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  /// Records a given segment to disk and puts it in the cache.
  /// @param cs The segment to store.
  void store(cow<segment> const& cs);

  /// Loads a segment into memory after a cache miss.
  /// @param uuid The ID which could not be found in the cache.
  /// @returns The loaded segment.
  cow<segment> on_miss(uuid const& id);

  path const dir_;
  util::lru_cache<uuid, cow<segment>> cache_;
  std::unordered_map<uuid, path> segment_files_;
};

} // namespace vast

#endif
