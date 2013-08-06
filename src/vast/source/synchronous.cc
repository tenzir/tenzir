#include "vast/source/synchronous.h"

#include "vast/logger.h"

namespace vast {
namespace source {

using namespace cppa;

synchronous::synchronous(actor_ptr sink, size_t batch_size)
  : sink_(sink),
    batch_size_(batch_size)
{
}

void synchronous::init()
{
  chaining(false);
  become(
      on(atom("batch size"), arg_match) >> [=](size_t batch_size)
      {
        batch_size_ = batch_size;
      },
      on(atom("kill")) >> [=]
      {
        quit();
      },
      on(atom("run")) >> [=]
      {
        run();
      },
      others() >> [=]
      {
        VAST_LOG_ERROR("source @" << id() <<
                       " received unexpected message from @" <<
                       last_sender()->id() << ": " <<
                       to_string(last_dequeued()));
      });
}

void synchronous::on_exit()
{
  VAST_LOG_VERBOSE("source @" << id() << " terminated");
}

void synchronous::run()
{
  VAST_ENTER();
  while (events_.size() < batch_size_)
  {
    if (finished())
    {
      break;
    }
    else if (auto e = extract())
    {
      events_.push_back(std::move(*e));
    }
    else
    {
      ++errors_;
      if (errors_ < 1000)
      {
        VAST_LOG_ERROR("source @" << id() << " experienced extraction error");
      }
      else if (errors_ == 1000)
      {
        VAST_LOG_ERROR("source @" << id() << " won't report further errors");
      }
    }
  }

  if (! events_.empty())
  {
    VAST_LOG_DEBUG("source @" << id() << " sends " << events_.size() <<
                   " events to sink @" << sink_->id());

    send(sink_, std::move(events_));
    events_.clear();
  }

  send(self, finished() ? atom("kill") : atom("run"));
}

} // namespace source
} // namespace vast
