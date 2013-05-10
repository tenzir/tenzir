#ifndef VAST_SEGMENTIZER_H
#define VAST_SEGMENTIZER_H

#include <cppa/cppa.hpp>
#include "vast/segment.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Receives events from sources, writes them into segments, and then relays
/// them upstream.
class segmentizer : public cppa::event_based_actor
{
public:
  /// Spawns a segmentizer.
  ///
  /// @param upstream The upstream actor receiving the generated segments.
  ///
  /// @param source The source to receive events from and take ownership of.
  ///
  /// @param max_events_per_chunk The maximum number of events to put in a
  /// single chunk.
  ///
  /// @param max_segment_size The maximum number of bytes to put in a single
  /// segment.
  segmentizer(cppa::actor_ptr upstream, cppa::actor_ptr source,
              size_t max_events_per_chunk, size_t max_segment_size);

  /// Implements `cppa::event_based_actor::init`.
  virtual void init() final;

private:
  size_t total_events_ = 0;
  unsigned wait_attempts_ = 0;

  size_t writer_bytes_at_last_rotate_ = 0;
  util::temporal_accumulator<size_t> stats_;
  segment segment_;
  segment::writer writer_;

  cppa::behavior operating_;
};

} // namespace vast

#endif
