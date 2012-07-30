#ifndef VAST_SEGMENTIZER_H
#define VAST_SEGMENTIZER_H

#include <cppa/cppa.hpp>
#include "vast/segment.h"

namespace vast {

/// Writes events into a segment and relays fresh segments to the sender.
class segmentizer : public cppa::sb_actor<segmentizer>
{
  friend class cppa::sb_actor<segmentizer>;

public:
  /// Spawns a segmentizer.
  /// @param max_events_per_chunk The maximum number of events per chunk.
  /// @param max_segment_size The maximum segment size in bytes.
  segmentizer(size_t max_events_per_chunk, size_t max_segment_size);

private:
  void write_event(const ze::event& event);

  size_t last_bytes_ = 0;
  segment segment_;
  segment::writer writer_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
