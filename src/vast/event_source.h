#ifndef VAST_EVENT_SOURCE_H
#define VAST_EVENT_SOURCE_H

#include <deque>
#include <cppa/cppa.hpp>
#include <ze/forward.h>
#include "vast/segment.h"
#include "vast/util/accumulator.h"

namespace vast {

/// A source that transforms generates events.
class event_source : public cppa::sb_actor<event_source>
{
  friend class cppa::sb_actor<event_source>;

public:
  /// Spawns an event source.
  /// @param ingestor The ingestor.
  /// @param tracker The event ID tracker.
  event_source(cppa::actor_ptr ingestor, cppa::actor_ptr tracker);
  virtual ~event_source() = default;

protected:
  /// Extracts one event from the source.
  /// @return The extracted event.
  virtual ze::event extract() = 0;

  /// Indicates whether the source has finished.
  bool finished_ = true;

private:
  class segmentizer : public cppa::sb_actor<segmentizer>
  {
    friend class cppa::sb_actor<segmentizer>;
  public:
    segmentizer(size_t max_events_per_chunk,
                size_t max_segment_size,
                cppa::actor_ptr ingestor);

  private:
    void shutdown();

    segment segment_;
    segment::writer writer_;
    size_t writer_bytes_at_last_rotate_ = 0;
    cppa::behavior init_state;
  };

  void imbue(uint64_t lower, uint64_t upper);

  bool waiting_ = true;
  size_t errors_ = 0;
  size_t events_ = 0;
  util::temporal_accumulator<size_t> stats_;
  std::deque<std::vector<ze::event>> buffers_;
  cppa::actor_ptr segmentizer_;
  cppa::behavior init_state;
};


} // namespace vast

#endif
