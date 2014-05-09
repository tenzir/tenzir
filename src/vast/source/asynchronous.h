#ifndef VAST_SOURCE_ASYNCHRONOUS_H
#define VAST_SOURCE_ASYNCHRONOUS_H

#include <cassert>
#include <cppa/cppa.hpp>
#include "vast/event.h"

namespace vast {
namespace source {

/// An asynchronous source that buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
template <typename Derived>
class asynchronous : public cppa::event_based_actor
{
public:
  /// Spawns an asynchronous source.
  asynchronous(actor_ptr sink, size_t batch_size = 0)
    : sink_(sink),
      batch_size_(batch_size)
  {
    using namespace cppa;
    operating_ = (
        on(atom("batch size"), arg_match) >> [=](size_t batch_size)
        {
          batch_size_ = batch_size;
        },
        on_arg_match >> [=](event& e)
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
        on_arg_match >> [=](std::vector<event>& v)
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

  /// Implements `cppa::event_based_actor::init`.
  void init() override
  {
    become(operating_.or_else(static_cast<Derived*>(this)->impl_));
  }

  void send_events()
  {
    if (events_.size() < batch_size_)
      return;

    send(sink_, std::move(this->events_));
    this->events_.clear();
  }

private:
  cppa::actor_ptr sink_;
  size_t batch_size_ = 0;
  std::vector<event> events_;
  cppa::partial_function operating_;
};

} // namespace source
} // namespace vast

#endif
