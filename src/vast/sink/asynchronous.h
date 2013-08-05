#ifndef VAST_SINK_ASYNCHRONOUS_H
#define VAST_SINK_ASYNCHRONOUS_H

#include <cppa/cppa.hpp>

namespace vast {

class event;

namespace sink {

/// A sink that processes events synchronously.
class asynchronous : public cppa::event_based_actor
{
public:
  virtual void init() override;

protected:
  /// Processes one event.
  /// @param e The event to process.
  /// @return `true` if the sink processed the event successfully.
  virtual bool process(event const& e) = 0;

  /// Processes a sequence of events.
  /// @param v The vector of events to process process.
  /// @return `true` *iff* the sink processed all events successfully.
  virtual bool process(std::vector<event> const& v);

  size_t total_events_ = 0;
};


} // namespace sink
} // namespace vast

#endif
