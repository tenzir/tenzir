#ifndef VAST_EVENT_SINK_H
#define VAST_EVENT_SINK_H

#include <cppa/cppa.hpp>
#include <ze/forward.h>

namespace vast {

/// A sink that processes events.
class event_sink : public cppa::sb_actor<event_sink>
{
  friend class cppa::sb_actor<event_sink>;

public:
  /// Spawns an event sink.
  event_sink();

  virtual ~event_sink() = default;

protected:
  /// Processes one event.
  /// @param event The event to process.
  /// @return bool `true` if the sink processed the event successfully.
  virtual bool process(ze::event const& event) = 0;

  bool finished_ = true;
  size_t total_events_ = 0;

private:
  cppa::behavior init_state;
};


} // namespace vast

#endif
