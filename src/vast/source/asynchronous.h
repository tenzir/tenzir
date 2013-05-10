#ifndef VAST_SOURCE_ASYNCHRONOUS_H
#define VAST_SOURCE_ASYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include <ze/event.h>

namespace vast {
namespace source {

/// An asynchronous source that buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
template <typename Derived>
class asynchronous : public cppa::event_based_actor
{
public:
  /// Spawns an asynchronous source.
  asynchronous()
  {
    using namespace cppa;
    operating_ = (
        on(atom("init"), arg_match) >> [=](actor_ptr upstream,
                                           size_t batch_size)
        {
          upstream_ = upstream;
          batch_size_ = batch_size;
        },
        on_arg_match >> [=](ze::event& e)
        {
          this->events_.push_back(std::move(e));
          if (events_.size() < batch_size)
            return;
          send(upstream, std::move(this->events_));
          this->events_.clear();
        },
        on_arg_match >> [=](std::vector<ze::event> v)
        {
          assert(! "not yet implemented");
        });
  }

  /// Implements `cppa::event_based_actor::init`.
  void init() override
  {
    become(operating_.or_else(static_cast<Derived*>(this)->impl));
  }

private:
  size_t batch_size_;
  cppa::actor_ptr upstream_;
  std::vector<ze::event> events_;
  cppa::partial_function operating_;
};

} // namespace source
} // namespace vast

#endif
