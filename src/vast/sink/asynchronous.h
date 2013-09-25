#ifndef VAST_SINK_ASYNCHRONOUS_H
#define VAST_SINK_ASYNCHRONOUS_H

#include "vast/actor.h"

namespace vast {

class event;

namespace sink {

/// A sink that processes events asynchronously.
class asynchronous : public actor<asynchronous>
{
public:
  virtual void on_exit() final;
  void act();
  virtual char const* description() const = 0;

  /// Retrieves the total number of events processed.
  /// @returns The number of events this sink received.
  size_t total_events() const;

protected:
  /// Processes one event.
  /// @param e The event to process.
  /// @returns `true` if the sink processed the event successfully.
  virtual void process(event const& e) = 0;

  /// Processes a sequence of events.
  /// @param v The vector of events to process process.
  /// @returns `true` *iff* the sink processed all events successfully.
  virtual void process(std::vector<event> const& v);

  /// A hook which executes before the sink terminates.
  virtual void before_exit();

  size_t total_events_ = 0;
};

} // namespace sink
} // namespace vast

#endif
