#include "vast/segmentizer.h"

#include <ze/event.h>
#include "vast/logger.h"

namespace vast {

segmentizer::segmentizer(size_t max_events_per_chunk, size_t max_segment_size)
  : writer_(segment_)
{
  LOG(verbose, ingest) << "spawning segmentizer @" << id();
  LOG(verbose, ingest)
    << "segmentizer @" << id() << " has maximum segment size of "
    << max_segment_size << " bytes";
  LOG(verbose, ingest)
    << "segmentizer @" << id() << " uses at most "
    << max_events_per_chunk << " events per chunk";

  using namespace cppa;
  init_state = (
      on_arg_match >> [=](std::vector<ze::event> const& v)
      {
        for (auto& e : v)
        {
          auto n = writer_ << e;
          if (n % max_events_per_chunk != 0)
            continue;

          if (writer_.bytes() - last_bytes_ < max_segment_size)
          {
            writer_.flush_chunk();
            continue;
          }

          last_bytes_ = writer_.bytes();

          DBG(ingest)
            << "segmentizer @" << id() << " relays segment " << segment_.id();

          reply(std::move(segment_));
          segment_ = segment();
        }
      },
      on(atom("shutdown")) >> [=]
      {
        if (segment_.head().events > 0)
        {
          if (writer_.elements() > 0)
            writer_.flush_chunk();

          reply(std::move(segment_));
        }
        else
        {
          reply(atom("done"));
        }

        quit();
        LOG(verbose, ingest) << "segmentizer @" << id() << " terminated";
      });
}

} // namespace vast
