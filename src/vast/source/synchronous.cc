#include "vast/source/synchronous.h"

namespace vast {
namespace source {

using namespace cppa;

synchronous::synchronous(actor_ptr sink, size_t batch_size)
  : sink_{std::move(sink)},
    batch_size_{batch_size}
{
  chaining(false);
}

void synchronous::act()
{
  become(
      on(atom("batch size"), arg_match) >> [=](size_t batch_size)
      {
        batch_size_ = batch_size;
      },
      on(atom("run")) >> [=]
      {
        run();
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR(description(),
                             "received unexpected message from @" <<
                             last_sender()->id() << ": " <<
                             to_string(last_dequeued()));
      });
}

void synchronous::run()
{
  VAST_ENTER();
  while (events_.size() < batch_size_)
  {
    if (finished())
      break;
    else if (auto e = extract())
      events_.push_back(std::move(*e));
    else if (++errors_ % 100 == 0)
      VAST_LOG_ACTOR_ERROR(description(), "failed on " << errors_ << " events");
  }

  if (! events_.empty())
  {
    VAST_LOG_ACTOR_DEBUG(description(), "sends " << events_.size() <<
                         " events to sink @" << sink_->id());

    send(sink_, std::move(events_));
    events_.clear();
  }

  if (finished())
    quit(exit::done);
  else
    send(self, atom("run"));
}

} // namespace source
} // namespace vast
