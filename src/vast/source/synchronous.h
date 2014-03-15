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
struct synchronous : public actor<synchronous<Derived>>
{
public:
  /// Spawns a synchronous source.
  /// @param The actor receiving the generated events.
  /// @param batch_size The number of events to extract in one batch.
  synchronous(cppa::actor_ptr sink, uint64_t batch_size = 0)
    : sink_{std::move(sink)},
      batch_size_{batch_size}
  {
  }

  void act()
  {
    using namespace cppa;

    this->chaining(false);
    this->trap_exit(true);

    become(
        on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
        {
          send_events();
          this->quit(reason);
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
            result<event> r{static_cast<Derived*>(this)->extract()};
            if (r)
            {
              events_.push_back(std::move(*r));
            }
            else if (r.failed())
            {
              VAST_LOG_ACTOR_ERROR(r.failure().msg());
              done = true;
              break;
            }

            done = static_cast<Derived const*>(this)->done();
          }

          send_events();

          if (done)
            this->quit(exit::done);
          else
            send(self, atom("run"));
        },
        others() >> [=]
        {
          VAST_LOG_ACTOR_ERROR("received unexpected message from " <<
                               VAST_ACTOR_ID(this->last_sender()) << ": " <<
                               to_string(this->last_dequeued()));
        });
  }

  char const* description() const
  {
    return static_cast<Derived const*>(this)->description();
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

  cppa::actor_ptr sink_;
  uint64_t batch_size_ = 0;
  std::vector<event> events_;
};

} // namespace source
} // namespace vast

#endif
