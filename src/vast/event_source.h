#ifndef VAST_EVENT_SOURCE_H
#define VAST_EVENT_SOURCE_H

#include <cppa/cppa.hpp>
#include <ze/forward.h>

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
  /// Extracts one events from the source
  /// @return The extracted event.
  virtual ze::event extract() = 0;

  /// Asks the ID tracker for a batch of new IDs.
  /// @param n The number of IDs to request.
  void ask_for_new_ids(size_t n);

  /// Indicates whether the source has finished.
  bool finished_ = true;

private:
  uint64_t next_id_ = 0;
  uint64_t last_id_ = 0;
  size_t total_events_ = 0;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr segmentizer_;
  cppa::behavior init_state;
};


} // namespace vast

#endif
