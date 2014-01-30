#include "vast/segmentizer.h"

#include <cppa/cppa.hpp>
#include "vast/event.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

segmentizer::segmentizer(actor_ptr upstream,
                         size_t max_events_per_chunk, size_t max_segment_size)
  : upstream_{upstream},
    stats_{std::chrono::seconds(1)},
    segment_{uuid::random(), max_segment_size},
    writer_{&segment_, max_events_per_chunk}
{
}

void segmentizer::act()
{
  chaining(false);
  trap_exit(true);

  become(
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (! writer_.flush())
        {
          segment_ = segment{uuid::random()};
          writer_.attach_to(&segment_);
          if (! writer_.flush())
            VAST_LOG_ACTOR_ERROR("failed to flush a fresh segment");
          assert(segment_.events() > 0);
        }

        if (segment_.events() > 0)
        {
          VAST_LOG_ACTOR_DEBUG(
              "sends final segment " << segment_.id() << " with " <<
              segment_.events() << " events to " << VAST_ACTOR_ID(upstream_));

          send(upstream_, std::move(segment_));
        }

        if (total_events_ > 0)
          VAST_LOG_ACTOR_VERBOSE("processed " << total_events_ << " events");

        quit(reason);
      },
      on_arg_match >> [=](std::vector<event> const& v)
      {
        total_events_ += v.size();

        for (auto& e : v)
        {
          if (writer_.write(e))
          {
            if (stats_.increment())
            {
              send(upstream_, atom("statistics"), stats_.last());
              VAST_LOG_ACTOR_VERBOSE(
                  "ingests at rate " << stats_.last() << " events/sec" <<
                  " (mean " << stats_.mean() <<
                  ", median " << stats_.median() <<
                  ", standard deviation " << stats_.sd() << ")");
            }
          }
          else
          {
            VAST_LOG_ACTOR_DEBUG("sends segment " << segment_.id() <<
                                 " with " << segment_.events() <<
                                 " events to " << VAST_ACTOR_ID(upstream_));

            auto max_segment_size = segment_.max_bytes();
            send(upstream_, std::move(segment_));
            segment_ = segment{uuid::random(), max_segment_size};

            writer_.attach_to(&segment_);

            if (! writer_.flush())
            {
              VAST_LOG_ACTOR_ERROR("failed to flush chunk to fresh segment");
              quit(exit::error);
              return;
            }

            if (! writer_.write(e))
            {
              VAST_LOG_ACTOR_ERROR("failed to write event to fresh segment");
              quit(exit::error);
              return;
            }
          }
        }
      },
      others() >> [=]
      {
        VAST_LOG_ACTOR_ERROR(
            "received unexpected message from " <<
            VAST_ACTOR_ID(last_sender()) << ": " << to_string(last_dequeued()));
      });
}

char const* segmentizer::description() const
{
  return "segmentizer";
}


} // namespace vast
