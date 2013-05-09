#ifndef VAST_SOURCE_ASYNCHRONOUS_H
#define VAST_SOURCE_ASYNCHRONOUS_H

#include <ze/event.h>
#include <cppa/cppa.hpp>

namespace vast {
namespace source {

/// An asynchronous source that buffers and relays events in batches.
/// Any child deriving from this class must be an actor.
template <typename Derived>
struct asynchronous : public cppa::event_based_actor
{
  /// Constructs an asynchronous source.
  /// @param upstream The upstream of the event batches.
  /// @param batch_size The size of each event batch.
  asynchronous(cppa::actor_ptr upstream, size_t batch_size)
  {
    using namespace cppa;
    operating = (
        on_arg_match >> [=](ze::event& e)
        {
          this->buffer.push_back(std::move(e)); 
          if (buffer.size() < batch_size)
            return;
          send(upstream, std::move(this->buffer));
          this->buffer.clear();
        },
        on_arg_match >> [=](std::vector<ze::event> v)
        {
          // TODO: Implement append up to batch size.
          assert(! "not yet implemented");
        });
  }

  void init() override
  {
    become(operating.or_else(static_cast<Derived*>(this)->init_state));
  }

  std::vector<ze::event> buffer;
  cppa::partial_function operating;
};

} // namespace source
} // namespace vast

#endif
