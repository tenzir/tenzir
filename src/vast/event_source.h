#ifndef VAST_EVENT_SOURCE_H
#define VAST_EVENT_SOURCE_H

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

  /// Asks the ID tracker for a batch of new IDs.
  /// @param n The number of IDs to request.
  void ask_for_new_ids(size_t n);

  /// Indicates whether the source has finished.
  bool finished_ = true;

private:
  /// Writes an event into the current segment.
  void segmentize(ze::event const& e);

  /// Ships the current segment to the ingestor.
  void ship_segment();

  uint64_t next_id_ = 0;
  uint64_t last_id_ = 0;

  util::temporal_accumulator<size_t> events_;

  size_t max_events_per_chunk_ = 0;
  size_t max_segment_size_ = 0;
  size_t writer_bytes_at_last_rotate_ = 0;
  segment segment_;
  segment::writer writer_;

  cppa::actor_ptr ingestor_;
  cppa::actor_ptr tracker_;
  cppa::behavior init_state;
};


} // namespace vast

#endif
