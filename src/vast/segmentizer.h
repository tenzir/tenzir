#ifndef VAST_SEGMENTIZER_H
#define VAST_SEGMENTIZER_H

#include <deque>
#include <ze/fwd.h>
#include "vast/actor.h"
#include "vast/segment.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Receives events, writes them into segments, and sends them upstream after
/// having received an ID range for them from the tracker.
struct segmentizer : public actor<segmentizer>
{
  /// Spawns a segmentizer.
  ///
  /// @param upstream The upstream.
  ///
  /// @param tracker The event ID tracker.
  ///
  /// @param max_events_per_chunk The maximum number of events to put in a
  /// single chunk.
  ///
  /// @param max_segment_size The maximum number of bytes to put in a single
  /// segment.
  segmentizer(cppa::actor_ptr upstream, cppa::actor_ptr tracker,
              size_t max_events_per_chunk, size_t max_segment_size);

  size_t total_events_ = 0;
  unsigned wait_attempts_ = 0;

  size_t writer_bytes_at_last_rotate_ = 0;
  util::temporal_accumulator<size_t> stats_;
  std::deque<segment> pending_;
  segment segment_;
  segment::writer writer_;

  cppa::actor_ptr source_;
  cppa::behavior operating_;
};

} // namespace vast

#endif
