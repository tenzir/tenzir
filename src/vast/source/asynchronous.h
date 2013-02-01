#ifndef VAST_SOURCE_ASYNCHRONOUS_H
#define VAST_SOURCE_ASYNCHRONOUS_H

#include <ze/event.h>
#include "vast/actor.h"

namespace vast {
namespace source {

/// An asynchronous source that buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
template <typename Derived>
class asynchronous : public actor<asynchronous<Derived>>
{
public:
  /// Constructs an asynchronous source.
  /// @param upstream The upstream of the event batches.
  /// @param batch_size The size of each event batch.
  asynchronous(cppa::actor_ptr upstream, size_t batch_size)
  {
    using namespace cppa;
    processing_ = (
        on_arg_match >> [=](ze::event& event)
        {
          this->buffer_.push_back(std::move(event)); 
          if (buffer_.size() < batch_size_)
            return;
          send(upstream, std::move(this->events_));
          this->events_.clear();
        });
  }

  void init() override
  {
    become(this->operating().or_else(processing_));
  }

private:
  std::vector<ze::event> buffer_;
  cppa::partial_function processing_;
};

} // namespace source
} // namespace vast

#endif
