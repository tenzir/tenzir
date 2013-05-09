#include "vast/segmentizer.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

// TODO: factor chunky logic into functions.
segmentizer::segmentizer(cppa::actor_ptr upstream,
                         cppa::actor_ptr tracker,
                         size_t max_events_per_chunk,
                         size_t max_segment_size)
  : stats_(std::chrono::seconds(1)),
    segment_(ze::uuid::random()),
    writer_(&segment_)
{
  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](actor_ptr source)
      {
        source_ = source;
        monitor(source_);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        assert(last_sender() == source_);
        source_ = actor_ptr();
        send(self, atom("shutdown"));
      },
      on(atom("shutdown")) >> [=]
      {
        if (source_)
        {
          source_ << last_dequeued();
          return;
        }

        if (writer_.elements() > 0)
        {
          writer_.flush();
          auto n = segment_.events();
          pending_.push_back(std::move(segment_));
          send(tracker, atom("request"), n);
        }

        if (pending_.empty() || wait_attempts_ >= 10)
        {
          if (! pending_.empty())
          {
            size_t events = 0;
            for (auto& s : pending_)
              events += s.events();

            LOG(error, ingest)
              << "segmentizer @" << id()
              << " discards " << events << " events in "
              << pending_.size() << " pending segments";
          }

          quit();
          LOG(info, ingest)
            << "segmentizer @" << id() << " terminated (ingested "
            << events_ << " events";
        }
        else if (pending_.size() > 1)
        {
          ++wait_attempts_;
          LOG(info, ingest)
            << "segmentizer @" << id()
            << " waits " << wait_attempts_ * 2 << " seconds for " << pending_.size()
            << " outstanding id tracker repl"
            << (pending_.size() == 1 ? "y" : "ies")
            << " (" << wait_attempts_ << "/10 attempts)";

          delayed_send_tuple(self, std::chrono::seconds(wait_attempts_ * 2),
                             last_dequeued());
        }
      },
      on(atom("run")) >> [=]()
      {
        source_ << last_dequeued();
      },
      on_arg_match >> [=](std::vector<ze::event> const& events)
      {
        DBG(ingest)
          << "segmentizer @" << id()
          << " received " << events.size() << " events";

        for (auto& e : events)
        {
          writer_ << e;
          if (writer_.elements() % max_events_per_chunk != 0)
            continue;

          writer_.flush();

          if (writer_.chunk_bytes() - writer_bytes_at_last_rotate_ <
              max_segment_size)
            continue;

          writer_bytes_at_last_rotate_ = writer_.chunk_bytes();

          auto n = segment_.events();
          pending_.push_back(std::move(segment_));
          send(tracker, atom("request"), n);
          segment_ = segment(ze::uuid::random());
        }

        if (stats_.timed_add(events.size()) && stats_.last() > 0)
        {
          send(upstream_, atom("statistics"), stats_.last());
          LOG(verbose, ingest)
            << "segmentizer @" << id()
            << " ingests at rate " << stats_.last() << " events/sec"
            << " (mean " << stats_.mean()
            << ", median " << stats_.median()
            << ", standard deviation " << std::sqrt(stats_.variance())
            << ")";
        }
      },
      on(atom("id"), arg_match) >> [=](uint64_t lower, uint64_t upper)
      {
        DBG(ingest)
          << "segmentizer @" << id() << " received id range: "
          << '[' << lower << ',' << upper << ')';

        auto& s = pending_.front();
        auto n = upper - lower;
        if (n < s.events())
        {
          LOG(error, ingest)
            << "segmentizer @" << id()
            << " received fewer ids than required,"
            << " got: " << n << ", needed: " << s.events();
          throw error::ingest("not enough ids");
        }
        else if (n > s.events())
        {
          LOG(error, ingest)
            << "segmentizer @" << id()
            << " received more ids than required,"
            << " got: " << n << ", needed: " << s.events();
          throw error::ingest("too many ids");
        }

        s.head().base = lower;
        total_events_ += n;

        send(upstream_, std::move(s));
        pending_.pop_front();
      });
}

} // namespace vast
