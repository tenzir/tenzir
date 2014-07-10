#ifndef VAST_SOURCE_SYNCHRONOUS_H
#define VAST_SOURCE_SYNCHRONOUS_H

#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/event.h"
#include "vast/util/result.h"

namespace vast {
namespace source {

/// A synchronous source that extracts events one by one.
template <typename Derived>
struct synchronous : public actor_base
{
public:
  /// Spawns a synchronous source.
  /// @param The actor receiving the generated events.
  /// @param batch_size The number of events to extract in one batch.
  synchronous(cppa::actor sink, uint64_t batch_size = 0)
    : sink_{std::move(sink)},
      batch_size_{batch_size}
  {
  }

  cppa::partial_function act() final
  {
    using namespace cppa;

    this->trap_exit(true);

    attach_functor([=](uint32_t) { sink_ = invalid_actor; });

    return
    {
      [=](exit_msg const& e)
      {
        send_events();
        this->quit(e.reason);
      },
      on(atom("batch size"), arg_match) >> [=](uint64_t batch_size)
      {
        batch_size_ = batch_size;
      },
      on(atom("run")) >> [=]
      {
        bool done = false;
        while (events_.size() < batch_size_ && ! done)
        {
          result<event> r = static_cast<Derived*>(this)->extract();
          if (r)
          {
            events_.push_back(std::move(*r));
          }
          else if (r.failed())
          {
            VAST_LOG_ACTOR_ERROR(r.error());
            done = true;
            break;
          }

          done = static_cast<Derived const*>(this)->done();
        }

        send_events();

        if (done)
          this->quit(exit::done);
        else
          send(this, atom("run"));
      }
    };
  }

private:
  void send_events()
  {
    if (! events_.empty())
    {
      send(sink_, std::move(events_));
      events_.clear();
    }
  }

  cppa::actor sink_;
  uint64_t batch_size_ = 0;
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
