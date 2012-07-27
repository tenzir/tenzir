#ifndef VAST_EVENT_SOURCE_H
#define VAST_EVENT_SOURCE_H

#include <cppa/cppa.hpp>
#include <ze/forward.h>
#include <ze/value.h>

namespace vast {

/// A source that transforms generates events.
class event_source : public cppa::sb_actor<event_source>
{
  friend class cppa::sb_actor<event_source>;

public:
  /// Constructs a vent source.
  /// @param ingestor The ingestor.
  /// @param tracker The event ID tracker.
  event_source(cppa::actor_ptr ingestor, cppa::actor_ptr tracker);

  virtual ~event_source() = default;

protected:
  /// Extracts one events from the source
  /// @return The vector of extracted events.
  virtual ze::event extract() = 0;

//  /// Called by derived classes when a new event is ready to be shipped.
//  /// @param event The extracted event.
//  void handle(ze::event&& event) final;

  bool finished_ = true;

protected:
  /// Asks the ID tracker for a batch of new IDs.
  /// @param n The number of IDs to request.
  void ask_for_new_ids(size_t n);

  uint64_t next_id_ = 0;
  uint64_t last_id_ = 0;
  size_t total_events_ = 0;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr segmentizer_;
  cppa::behavior init_state;
};


} // namespace vast

#endif
