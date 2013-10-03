#ifndef VAST_SINK_SEGMENTIZER_H
#define VAST_SINK_SEGMENTIZER_H

#include "vast/segment.h"
#include "vast/sink/asynchronous.h"
#include "vast/util/accumulator.h"

namespace vast {
namespace sink {

/// Receives events from sources, writes them into segments, and then relays
/// them upstream.
class segmentizer : public asynchronous
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
  segmentizer(cppa::actor_ptr upstream,
              size_t max_events_per_chunk, size_t max_segment_size);

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

  virtual char const* description() const final;

protected:
  virtual void process(event const& e) override;

private:
  cppa::actor_ptr upstream_;
  util::temporal_accumulator<size_t> stats_;
  segment segment_;
  segment::writer writer_;
};

} // namespace sink
} // namespace vast

#endif
