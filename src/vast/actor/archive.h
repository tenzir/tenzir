#ifndef VAST_ACTOR_ARCHIVE_H
#define VAST_ACTOR_ARCHIVE_H

#include <unordered_map>
#include "vast/aliases.h"
#include "vast/chunk.h"
#include "vast/filesystem.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"
#include "vast/util/lru_cache.h"
#include "vast/util/range_map.h"

namespace vast {

/// Accepts chunks and constructs segments.
class archive : public flow_controlled_actor
{
public:
  struct chunk_compare
  {
    bool operator()(chunk const& lhs, chunk const& rhs) const
    {
      return lhs.meta().ids.find_first() < rhs.meta().ids.find_first();
    };
  };

  using segment = util::flat_set<chunk, chunk_compare>;

  /// Spawns the archive.
  /// @param dir The root directory of the archive.
  /// @param capacity The number of segments to hold in memory.
  /// @param max_segment_size The maximum size in MB of a segment.
  /// @pre `max_segment_size > 0`
  archive(path dir, size_t capacity, size_t max_segment_size);

  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  trial<void> store(segment s);
  trial<chunk> load(event_id eid);
  segment on_miss(uuid const& id);

  path dir_;
  size_t max_segment_size_;
  util::range_map<event_id, uuid> segments_;
  util::lru_cache<uuid, segment> cache_;
  segment current_;
  uint64_t current_size_;
  caf::actor accountant_;
};

} // namespace vast

#endif
