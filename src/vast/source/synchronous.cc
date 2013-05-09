#include "vast/source/synchronous.h"

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
      }
      on(atom("run")) >> [=]
      {
        while (! finished())
        {
          while (events_.size() < batch_size_)
          {
            if (auto event = extract())
            {
              events_.push_back(std::move(*event));
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

          send(upstream_, std::move(events_));
          events_.clear();

          // It could be that the call to extract() caused the source to finish.
          if (finished())
            break;
        }
      }
  );
}

} // namespace source
} // namespace vast
