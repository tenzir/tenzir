#ifndef VAST_ACTOR_SOURCE_ASYNCHRONOUS_H
#define VAST_ACTOR_SOURCE_ASYNCHRONOUS_H

#include <cassert>
#include "vast/event.h"
#include "vast/actor/actor.h"

namespace vast {
namespace source {

/// An asynchronous source that buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
template <typename Derived>
class asynchronous : public actor_base
{
public:
  /// Spawns an asynchronous source.
  asynchronous(actor_ptr sink, size_t batch_size = 0)
    : sink_(sink),
      batch_size_(batch_size)
  {
    using namespace caf;
    operating_ = (
        on(atom("batch size"), arg_match) >> [=](size_t batch_size)
        {
          batch_size_ = batch_size;
        },
        [=](event& e)
        {
          assert(sink_);
          if (batch_size_ == 0)
          {
            send(sink_, std::move(e));
            return;
          }

          this->events_.push_back(std::move(e));
          send_events();
        },
        [=](std::vector<event>& v)
        {
          assert(sink_);

          events_.insert(events_.end(),
              std::make_move_iterator(v.begin()),
              std::make_move_iterator(v.end()));

          std::inplace_merge(events_.begin(),
                             events_.begin() + events_.size(),
                             events_.end());
          send_events();
        });
  }

  /// Implements `caf::event_based_actor::init`.
  behavior act() final
  {
    attach_functor([=](uint32_t) { sink_ = invalid_actor; });

    return operating_.or_else(static_cast<Derived*>(this)->impl_);
  }

  void send_events()
  {
    if (events_.size() < batch_size_)
      return;

    send(sink_, std::move(this->events_));
    this->events_.clear();
  }

private:
  caf::actor_ptr sink_;
  size_t batch_size_ = 0;
  std::vector<event> events_;
  caf::message_handler operating_;
};

} // namespace source
} // namespace vast

#endif
