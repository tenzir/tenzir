#ifndef VAST_STORE_SEGMENTIZER_H
#define VAST_STORE_SEGMENTIZER_H

#include <cppa/cppa.hpp>
#include "vast/store/segment.h"

namespace vast {
namespace store {

/// Writes events into a segment and relays baked segments to the archive.
class segmentizer : public cppa::sb_actor<segmentizer>
{
  friend class cppa::sb_actor<segmentizer>;

public:
  /// Spawns a segmentizer.
  /// @param segment_manager The segment manager actor.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @param max_segment_size The maximum segment size in bytes.
  segmentizer(cppa::actor_ptr segment_manager,
              size_t max_events_per_chunk,
              size_t max_segment_size);

private:
  void terminate();

  segment segment_;
  segment::writer writer_;
  cppa::actor_ptr segment_manager_;
  cppa::behavior init_state;
};

} // namespace store
} // namespace vast

#endif
