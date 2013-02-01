#include "vast/source/synchronous.h"

namespace vast {
namespace source {

using namespace cppa;

synchronous::synchronous(actor_ptr upstream, size_t batch_size)
{
  operating_ = (
      on(atom("shutdown")) >> []()
      {
        quit();
        LOG(info, ingest) << "source @" << id() << " terminated";
      },
      on(atom("run")) >> [=]()
      {
        if (finished_)
          return;

        size_t extracted = 0;
        while (extracted < batch_size)
        {
          if (finished_)
            break;

          try
          {
            events_.push_back(extract());
            ++extracted;
          }
          catch (error::parse const& e)
          {
            ++errors_;
            if (errors_ < 1000)
            {
              LOG(error, ingest)
                << "source @" << id()
                << " encountered parse error: " << e.what();
            }
            else if (errors_ == 1000)
            {
              LOG(error, ingest)
                << "source @" << id() << " won't report further errors";
            }
          }
        }

        send(upstream, std::move(events_));
        events_.clear();

        if (finished_)
          send(self, atom("shutdown"));
        else
          send(self, atom("run"), batch_size);
      });
}

} // namespace source
} // namespace vast
