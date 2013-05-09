#ifndef VAST_SINK_SYNCHRONOUS_H
#define VAST_SINK_SYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include <ze/fwd.h>

namespace vast {

/// A sink that processes events.
class synchronous : public cppa::event_based_actor
{
public:
  virtual void init() override;

protected:
  /// Processes one event.
  /// @param e The event to process.
  /// @return bool `true` if the sink processed the event successfully.
  virtual bool process(ze::event const& e) = 0;

  size_t total_events_ = 0;
};


} // namespace vast

#endif
