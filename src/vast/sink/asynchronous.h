#ifndef VAST_SINK_ASYNCHRONOUS_H
#define VAST_SINK_ASYNCHRONOUS_H

#include <cppa/cppa.hpp>

namespace vast {

class event;

namespace sink {

/// A sink that processes events asynchronously.
class asynchronous : public cppa::event_based_actor
{
public:
  virtual void init() override;
  virtual void on_exit() override;

  /// Retrieves the total number of events processed.
  /// @return The number of events this sink received.
  size_t total_events() const;

protected:
  /// Processes one event.
  /// @param e The event to process.
  /// @return `true` if the sink processed the event successfully.
  virtual void process(event const& e) = 0;

  /// Processes a sequence of events.
  /// @param v The vector of events to process process.
  /// @return `true` *iff* the sink processed all events successfully.
  virtual void process(std::vector<event> const& v);

  /// A hook which executes before the sink terminates.
  virtual void before_exit();

  size_t total_events_ = 0;
};


} // namespace sink
} // namespace vast

#endif
