#include "vast/receiver.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

receiver::receiver(actor_ptr ingestor, actor_ptr tracker)
  : segment_(ze::uuid::random()),
    writer_(&segment_)
{
  operating_ = (
      on(atom("shutdown")) >> [=]
      {
        if (segment_.events() == 0)
        {
          shutdown();
        }
        else
        {
          DBG(ingest)
            << "segmentizer @" << id()
            << " ships final segment " << segment_.id()
            << " to ingestor @" << ingestor->id()
            << " (" << segment_.events() << " events)";

          if (writer_.elements() > 0)
            writer_.flush();

          sync_send(ingestor, std::move(segment_)).then(
              on(atom("segment"), atom("ack"), arg_match)
                >> [=](ze::uuid const& id)
              {
                shutdown();
              },
              after(std::chrono::seconds(10)) >> [=]
              {
                LOG(error, ingest)
                  << "segmentizer @" << id()
                  << " did not receive ack from ingestor @" << ingestor->id()
                  << " for " << segment_.id() << " after 10 seconds";
              });
        }
      });
}

void receiver::segmentizer::shutdown()
{
  quit();
  LOG(verbose, ingest) << "segmentizer @" << id() << " terminated";
}

receiver::receiver(cppa::actor_ptr ingestor, cppa::actor_ptr tracker)
  : stats_(std::chrono::seconds(1))
{
  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size)
      {
        max_events_per_chunk_ = max_events_per_chunk;
        max_segment_size_ = max_segment_size;
      },
      on(atom("run"), arg_match) >> [=](size_t batch_size)
      {
        worker_ << last_dequeued();
      },
      on_arg_match >> [=](std::vector<ze::event> const& events)
      {
        DBG(ingest)
          << "event source @" << id()
          << " received " << events.size() << " events";

        for (auto& e : events)
        {
          writer_ << e;
          if (writer_.elements() % max_events_per_chunk_ != 0)
            continue;

          writer_.flush();

          if (writer_.chunk_bytes() - writer_bytes_at_last_rotate_ < max_segment_size_)
            continue;

          writer_bytes_at_last_rotate_ = writer_.chunk_bytes();

          auto n = segment_.events();
          pending_.push_back(std::move(segment_));
          send(tracker, atom("request"), n);
          segment_ = segment(ze::uuid::random());
        }

        if (stats_.timed_add(events.size()) && stats_.last() > 0)
        {
          send(ingestor_, atom("statistics"), stats_.last());
          LOG(verbose, ingest)
            << "event source @" << id()
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
          << "event source @" << id() << " received id range: "
          << '[' << lower << ',' << upper << ')';

        auto& s = pending_.front();
        auto n = upper - lower;
        if (n < s.events())
        {
          LOG(error, ingest)
            << "event source @" << id()
            << " received fewer ids than required,"
            << " got: " << n << ", needed: " << s.events();
          throw error::ingest("not enough ids");
        }
        else if (n > s.events())
        {
          LOG(error, ingest)
            << "event source @" << id()
            << " received more ids than required,"
            << " got: " << n << ", needed: " << s.events();
          throw error::ingest("too many ids");
        }

        s.head().base = lower;
        total_events_ += n;

        send(ingestor_, std::move(s));
        pending_.pop_front();
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        assert(last_sender() == worker_);
        worker_ = actor_ptr();
        send(self, atom("shutdown"));
      },
      on(atom("shutdown")) >> [=]
      {
        if (worker_)
        {
          worker_ << last_dequeued();
          return;
        }

        if (pending_.empty() || wait_attempts_ >= 10)
        {
          if (! pending_.empty())
          {
            size_t events = 0;
            for (auto& s : pending_)
              events += s.events();

            LOG(error, ingest)
              << "event source @" << id()
              << " discards " << events << " events in "
              << pending_.size() << " pending segments";
          }

          quit();
          LOG(info, ingest)
            << "event source @" << id() << " terminated (ingested "
            << events_ << " events";
        }
        else if (pending_.size() > 1)
        {
          ++wait_attempts_;
          LOG(info, ingest)
            << "event source @" << id()
            << " waits " << wait_attempts_ * 2 << " seconds for " << pending_.size()
            << " outstanding id tracker repl"
            << (pending_.size() == 1 ? "y" : "ies")
            << " (" << wait_attempts_ << "/10 attempts)";

          delayed_send_tuple(self, std::chrono::seconds(wait_attempts_ * 2),
                             last_dequeued());
        }
      });
}

void receiver::imbue(uint64_t lower, uint64_t upper)
{
}

} // namespace vast
