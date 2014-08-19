#ifndef VAST_ARCHIVE_H
#define VAST_ARCHIVE_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/segment.h"
#include "vast/util/range_map.h"
#include "vast/util/lru_cache.h"

namespace vast {

/// Accepts chunks and constructs segments.
class archive : public actor_base
{
public:
  /// Spawns the archive.
  /// @param dir The root directory of the archive.
  /// @param capacity The number of segments to hold in memory.
  /// @param max_segment_size The maximum size in MB of a segment.
  /// @pre `max_segment_size > 0`
  archive(path dir, size_t capacity, size_t max_segment_size);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  bool store(caf::message msg);
  trial<caf::message> load(event_id eid);
  caf::message on_miss(uuid const& id);

  path dir_;
  size_t max_segment_size_;
  util::range_map<event_id, uuid> ranges_;
  util::lru_cache<uuid, caf::message> cache_;
  std::unordered_map<uuid, path> segment_files_;
  segment current_;
  uint64_t current_size_;
};

} // namespace vast

#endif
