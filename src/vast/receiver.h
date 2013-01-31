#ifndef VAST_EVENT_SOURCE_H
#define VAST_EVENT_SOURCE_H

#include <deque>
#include <ze/fwd.h>
#include "vast/actor.h"
#include "vast/segment.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Receives events and writes them into segments.
struct receiver : public actor<receiver>
{
  /// Spawns an event source.
  /// @param ingestor The ingestor.
  /// @param tracker The event ID tracker.
  receiver(actor_ptr ingestor, actor_ptr tracker);

  size_t total_events_ = 0;
  unsigned wait_attempts_ = 0;
  util::temporal_accumulator<size_t> stats_;
  std::deque<segment> pending_;
  segment segment_;
  segment::writer writer_;
  size_t writer_bytes_at_last_rotate_ = 0;
  behavior operating_;
};

} // namespace vast

#endif
