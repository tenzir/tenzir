#include "vast/event_source.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

using namespace cppa;

event_source::segmentizer::segmentizer(
    size_t max_events_per_chunk,
    size_t max_segment_size,
    actor_ptr ingestor)
  : segment_(ze::uuid::random())
  , writer_(&segment_)
{
  init_state = (
      on_arg_match >> [=](std::vector<ze::event> const& events)
      {
        DBG(ingest)
          << "segmentizer @" << id()
          << " received " << events.size() << " events";

        for (auto& e : events)
        {
          auto n = writer_ << e;
          if (n % max_events_per_chunk != 0)
            continue;

          writer_.flush_chunk();

          if (writer_.bytes() - writer_bytes_at_last_rotate_ < max_segment_size)
            continue;

          writer_bytes_at_last_rotate_ = writer_.bytes();

          DBG(ingest)
            << "segmentizer @" << id()
            << " ships segment " << segment_.id()
            << " to ingestor @" << ingestor->id()
            << " (" << segment_.events() << " events)";

          sync_send(ingestor, std::move(segment_)).then(
              on(atom("segment"), atom("ack"), arg_match)
                >> [=](ze::uuid const& uuid)
              {
                assert(last_sender() == ingestor);
                DBG(ingest)
                  << "segmentizer @" << id()
                  << " received segment ack from ingestor @" << ingestor->id()
                  << " for " << uuid;
              },
              after(std::chrono::seconds(10)) >> [=]
              {
                LOG(error, ingest)
                  << "segmentizer @" << id()
                  << " did not receive ack from ingestor @" << ingestor->id()
                  << " for " << segment_.id() << " after 10 seconds";
              });

          segment_ = segment(ze::uuid::random());
        }
      },
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
            writer_.flush_chunk();

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

void event_source::segmentizer::shutdown()
{
  quit();
  LOG(verbose, ingest) << "segmentizer @" << id() << " terminated";
}

event_source::event_source(cppa::actor_ptr ingestor, cppa::actor_ptr tracker)
  : stats_(std::chrono::seconds(1))
  , buffers_(1)
{
  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size)
      {
        segmentizer_ = spawn<segmentizer>(
            max_events_per_chunk,
            max_segment_size,
            ingestor);

        monitor(segmentizer_);

        LOG(verbose, ingest)
          << "event source @" << id()
          << " spawns segmentizer @" << segmentizer_->id()
          << " with ingestor @" << ingestor->id();
      },
      on(atom("extract"), arg_match) >> [=](size_t n)
      {
        if (finished_)
        {
          send(self, atom("shutdown"));
          return;
        }

        assert(! buffers_.empty());
        auto& buffer = buffers_.back();
        size_t extracted = 0;
        while (extracted < n)
        {
          if (finished_)
            break;

          try
          {
            auto e = extract();
            buffer.push_back(std::move(e));
            ++extracted;
          }
          catch (error::parse const& e)
          {
            LOG(error, ingest) << e.what();
          }
        }

        if (stats_.timed_add(extracted) && stats_.last() > 0)
        {
          DBG(ingest)
            << "event source @" << id() << " asks tracker @"  << tracker->id()
            << " for " << buffer.size() << " ids";

          send(tracker, atom("request"), buffer.size());
          buffers_.push_back({});

          LOG(info, ingest)
            << "event source @" << id()
            << " ingests at rate " << stats_.last() << " events/sec"
            << " (mean " << stats_.mean()
            << ", median " << stats_.median()
            << ", variance " << stats_.variance()
            << ")";
        }

        if (finished_)
          send(self, atom("shutdown"));
        else
          send(ingestor, atom("source"), atom("ack"), extracted);
      },
      on(atom("id"), arg_match) >> [=](uint64_t lower, uint64_t upper)
      {
        DBG(ingest)
          << "event source @" << id() << " received id range: "
          << '[' << lower << ',' << upper << ')';

        imbue(lower, upper);
      },
      on(atom("shutdown")) >> [=]
      {
        finished_ = true;
        if (buffers_.empty())
        {
          send(segmentizer_, atom("shutdown"));
        }
        else if (! waiting_)
        {
          send(segmentizer_, atom("shutdown"));
          LOG(error, ingest)
            << "event source @" << id()
            << " terminates, discarding buffered events";
        }
        else if (buffers_.size() > 1)
        {
          LOG(info, ingest)
            << "event source @" << id()
            << " waits 3 seconds for " << buffers_.size()
            << " outstanding tracker replies";

          delayed_send(self, std::chrono::seconds(3), atom("shutdown"));
          waiting_ = false;
        }
        else
        {
          auto& buffer = buffers_.front();
          DBG(ingest)
            << "event source @" << id() << " synchronously asks tracker for "
            << buffer.size() << " ids";

          sync_send(tracker, atom("request"), buffer.size()).then(
              on(atom("id"), arg_match) >> [=](uint64_t lower, uint64_t upper)
              {
                DBG(ingest)
                  << "event source @" << id() << " received id range: "
                  << '[' << lower << ',' << upper << ')';

                imbue(lower, upper);
                send(segmentizer_, atom("shutdown"));
              },
              after(std::chrono::seconds(10)) >> [=]
              {
                LOG(error, ingest)
                  << "segmentizer @" << id()
                  << " timed out after 10 seconds trying to contact tracker";

                buffers_.pop_front();
              });
        }
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        // We can only terminate after the segmentizer has delivered all
        // outstanding segments and terminated, which is witnessed by the
        // arrival of this message.

        if (! buffers_.empty())
        {
          size_t events = 0;
          for (auto& buf : buffers_)
            events += buf.size();

          LOG(warn, ingest)
            << "event source @" << id()
            << " discards " << events << " events in "
            << buffers_.size() << " segment buffers";
        }

        quit();
        LOG(verbose, ingest)
          << "event source @" << id()
          << " terminated (ingested " << total_events_ << " events)";
      });
}

void event_source::imbue(uint64_t lower, uint64_t upper)
{
  assert(! buffers_.empty());

  auto& buffer = buffers_.front();
  auto n = upper - lower;
  if (n < buffer.size())
  {
    LOG(error, ingest)
      << "event source @" << id()
      << " received fewer ids than required,"
      << " got: " << n << ", buffered: " << buffer.size();

    throw error::ingest("not enough ids");
  }

  for (size_t i = 0; i < static_cast<size_t>(n); ++i)
    buffer[i].id(lower++);

  total_events_ += buffer.size();
  send(segmentizer_, std::move(buffer));
  buffers_.pop_front();
}

} // namespace vast
