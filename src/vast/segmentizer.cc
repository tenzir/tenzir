#include "vast/segmentizer.h"

#include "vast/event.h"
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

segmentizer::segmentizer(actor_ptr upstream, actor_ptr source,
                         size_t max_events_per_chunk,
                         size_t max_segment_size)
  : stats_(std::chrono::seconds(1)),
    segment_(uuid::random()),
    writer_(&segment_)
{
  monitor(source);
  operating_ = (
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        VAST_LOG_DEBUG("segmentizer @" << id() << " received DOWN from @" <<
                       last_sender()->id());

        if (writer_.elements() > 0)
        {
          writer_.flush();
          auto n = segment_.events();
          total_events_ += n;
          send(upstream, std::move(segment_));
          VAST_LOG_DEBUG("segmentizer @" << id() <<
                         " forwarded last segment with " << n << " events");
        }

        VAST_LOG_VERBOSE("segmentizer @" << id() << " processed a total of " <<
                         total_events_ << " events");

        quit();
        VAST_LOG_VERBOSE("segmentizer @" << id() << " terminated");
      },
      on(atom("kill")) >> [=]
      {
        source << last_dequeued();
      },
      on_arg_match >> [=](std::vector<event> const& events)
      {
        VAST_LOG_DEBUG("segmentizer @" << id() <<
                       " received " << events.size() << " events");

        for (auto& e : events)
        {
          ++total_events_;

          writer_ << e;
          if (writer_.elements() % max_events_per_chunk != 0)
            continue;
          writer_.flush();
          if (writer_.chunk_bytes() - writer_bytes_at_last_rotate_ <
              max_segment_size)
            continue;
          writer_bytes_at_last_rotate_ = writer_.chunk_bytes();

          auto n = segment_.events();
          send(upstream, std::move(segment_));
          segment_ = segment(uuid::random());

          VAST_LOG_DEBUG("segmentizer @" << id() <<
                         " forwarded last segment with " << n << " events");
        }

        if (stats_.timed_add(events.size()) && stats_.last() > 0)
        {
          send(upstream, atom("statistics"), stats_.last());
          VAST_LOG_VERBOSE(
              "segmentizer @" << id() <<
              " ingests at rate " << stats_.last() << " events/sec" <<
              " (mean " << stats_.mean() <<
              ", median " << stats_.median() <<
              ", standard deviation " << std::sqrt(stats_.variance()) << ")");
        }
      });
}

void segmentizer::init()
{
  become(operating_);
}

} // namespace vast
