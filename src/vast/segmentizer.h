#ifndef VAST_SEGMENTIZER_H
#define VAST_SEGMENTIZER_H

#include "vast/actor.h"
#include "vast/segment.h"
#include "vast/util/accumulator.h"

namespace vast {

/// Receives events from sources, writes them into segments, and then relays
/// them upstream.
class segmentizer : public actor_base
{
public:
  /// Spawns a segmentizer.
  ///
  /// @param upstream The upstream actor receiving the generated segments.
  ///
  /// @param max_events_per_chunk The maximum number of events to put in a
  /// single chunk.
  ///
  /// @param max_segment_size The maximum number of bytes to put in a single
  /// segment.
  segmentizer(caf::actor upstream,
              size_t max_events_per_chunk, size_t max_segment_size);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  caf::actor upstream_;
  util::rate_accumulator<uint64_t> stats_;
  segment segment_;
  segment::writer writer_;
  size_t total_events_ = 0;
};

} // namespace vast

#endif
