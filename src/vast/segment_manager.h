#ifndef VAST_SEGMENT_MANAGER_H
#define VAST_SEGMENT_MANAGER_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include <ze/file_system.h>
#include <ze/uuid.h>
#include "vast/util/lru_cache.h"

namespace vast {

// Forward declarations.
class segment;

/// Manages the segments on disk an in-memory segments in a LRU fashion.
class segment_manager : public cppa::sb_actor<segment_manager>
{
  friend class cppa::sb_actor<segment_manager>;
  typedef util::lru_cache<ze::uuid, cppa::cow_tuple<segment>> lru_cache;

public:
  /// Spawns the segment manager.
  ///
  /// @param capacity The number of segments to keep in memory until old ones
  /// should be evicted.
  ///
  /// @param dir The directory with the segments.
  segment_manager(size_t capacity, std::string const& dir);

private:
  /// Records a given segment to disk and puts it in the cache.
  /// @param t The COW-tuple containing the segment.
  void store_segment(cppa::cow_tuple<segment> t);

  /// Loads a segment into memory after a cache miss.
  /// @param uuid The ID which could not be found in the cache.
  /// @return A copy-on-write tuple containing the loaded segment.
  cppa::cow_tuple<segment> on_miss(ze::uuid const& uuid);

  lru_cache cache_;
  ze::path const dir_;
  std::unordered_map<ze::uuid, ze::path> segment_files_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
