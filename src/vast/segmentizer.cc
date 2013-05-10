#include "vast/segmentizer.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

segmentizer::segmentizer(actor_ptr upstream, actor_ptr source,
                         size_t max_events_per_chunk,
                         size_t max_segment_size)
  : stats_(std::chrono::seconds(1)),
    segment_(ze::uuid::random()),
    writer_(&segment_)
{
  monitor(source);
  operating_ = (
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        if (writer_.elements() > 0)
        {
          writer_.flush();
          auto n = segment_.events();
          total_events_ += n;
          pending_.push_back(std::move(segment_));
          send(upstream, std::move(segment_));
          LOG(debug, ingest)
            << "segmentizer @" << id()
            << " forwarded last segment with " << n << " events";
        }

        LOG(verbose, ingest)
          << "segmentizer @" << id()
          << " processed a total of " << total_events_ << " events";

        quit();
        LOG(verbose, ingest) << "segmentizer @" << id() << " terminated";
      },
      on(atom("kill")) >> [=]
      {
        source << last_dequeued();
      },
      on_arg_match >> [=](std::vector<ze::event> const& events)
      {
        LOG(debug, ingest)
          << "segmentizer @" << id()
          << " received " << events.size() << " events";

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
          segment_ = segment(ze::uuid::random());

          LOG(debug, ingest)
            << "segmentizer @" << id()
            << " forwarded last segment with " << n << " events";
        }

        if (stats_.timed_add(events.size()) && stats_.last() > 0)
        {
          send(upstream, atom("statistics"), stats_.last());
          LOG(verbose, ingest)
            << "segmentizer @" << id()
            << " ingests at rate " << stats_.last() << " events/sec"
            << " (mean " << stats_.mean()
            << ", median " << stats_.median()
            << ", standard deviation " << std::sqrt(stats_.variance())
            << ")";
        }
      });
}

void segmentizer::init()
{
  become(operating_);
}

} // namespace vast
