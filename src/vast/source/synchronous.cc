#include "vast/source/synchronous.h"

namespace vast {
namespace source {

using namespace cppa;

synchronous::synchronous(actor_ptr sink, size_t batch_size)
  : sink_{std::move(sink)},
    batch_size_{batch_size}
{
}

void synchronous::act()
{
  chaining(false);
  trap_exit(true);
  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        send_events();
        quit(reason);
      },
      on(atom("batch size"), arg_match) >> [=](size_t batch_size)
      {
        batch_size_ = batch_size;
      },
      on(atom("run")) >> [=]
      {
        while (events_.size() < batch_size_)
        {
          if (finished())
            break;
          else if (auto e = extract())
            events_.push_back(std::move(*e));
          else if (++errors_ % 100 == 0)
            VAST_LOG_ACTOR_ERROR(description(),
                                 "failed on " << errors_ << " events");
        }

        send_events();

        if (finished())
          quit(exit::done);
        else
          send(self, atom("run"));
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR(description(),
                             "received unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

void synchronous::send_events()
{
  if (! events_.empty())
  {
    VAST_LOG_ACTOR_DEBUG(description(), "sends " << events_.size() <<
                         " events to sink @" << sink_->id());
    send(sink_, std::move(events_));
    events_.clear();
  }
}

} // namespace source
} // namespace vast
