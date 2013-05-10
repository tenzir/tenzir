#include "vast/source/synchronous.h"

#include "vast/logger.h"

namespace vast {
namespace source {

using namespace cppa;

void synchronous::init()
{
  become(
      on(atom("init"), arg_match) >> [=](actor_ptr upstream, size_t batch_size)
      {
        upstream_ = upstream;
        batch_size_ = batch_size;
      },
      on(atom("kill")) >> [=]
      {
        quit();
        LOG(verbose, ingest) << "source @" << id() << " terminated";
      },
      on(atom("run")) >> [=] { run(); }
  );
}

void synchronous::run()
{
  while (events_.size() < batch_size_)
  {
    if (auto e = extract())
    {
      events_.push_back(std::move(*e));
    }
    else if (finished())
    {
      break;
    }
    else
    {
      ++errors_;
      if (errors_ < 1000)
      {
        LOG(error, ingest)
          << "source @" << id() << " encountered parse error";
      }
      else if (errors_ == 1000)
      {
        LOG(error, ingest)
          << "source @" << id() << " won't report further errors";
      }
    }
  }

  if (! events_.empty())
  {
    send(upstream_, std::move(events_));
    events_.clear();
  }

  send(self, finished() ? atom("kill") : atom("run"));
}

} // namespace source
} // namespace vast
