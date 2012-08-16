#include "vast/event_source.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/logger.h"

namespace vast {

event_source::event_source(cppa::actor_ptr ingestor, cppa::actor_ptr tracker)
  : events_(std::chrono::seconds(1))
  , writer_(segment_)
  , ingestor_(ingestor)
  , tracker_(tracker)
{
  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size)
      {
        max_events_per_chunk_ = max_events_per_chunk;
        max_segment_size_ = max_segment_size;
      },
      on(atom("extract"), arg_match) >> [=](size_t n)
      {
        if (finished_)
        {
          send(self, atom("shutdown"));
          return;
        }

        if (next_id_ == last_id_)
        {
          ask_for_new_ids(n);
          return;
        }

        size_t extracted = 0;
        while (extracted < n)
        {
          if (finished_)
            break;

          try
          {
            auto e = extract();
            assert(next_id_ != last_id_);
            e.id(next_id_++);
            segmentize(e);
            ++extracted;
            ++total_events_;
          }
          catch (error::parse const& e)
          {
            LOG(error, ingest) << e.what();
          }
        }

        if (events_.timed_add(extracted) && events_.last() > 0)
        {
          LOG(info, ingest)
            << "event source @" << id()
            << " ingests at rate " << events_.last() << " events/sec"
            << " (mean " << events_.mean()
            << ", median " << events_.median()
            << ", variance " << events_.variance()
            << ")";
        }

        if (finished_)
          send(self, atom("shutdown"));
        else
          send(ingestor_, atom("source"), atom("ack"), extracted);
      },
      on(atom("shutdown")) >> [=]
      {
        if (segment_.events() > 0)
        {
          if (writer_.elements() > 0)
            writer_.flush_chunk();

          ship_segment();
        }

        send(ingestor_, atom("shutdown"), atom("ack"), total_events_);

        quit();
        LOG(verbose, ingest) << "event source @" << id() << " terminated";
      });
}

void event_source::ask_for_new_ids(size_t n)
{
  DBG(ingest)
    << "event source @" << id()
    << " asks tracker @"  << tracker_->id()
    << " for " << n << " new ids";

  using namespace cppa;
  auto future = sync_send(tracker_, atom("request"), n);
  handle_response(future)(
      on(atom("id"), arg_match) >> [=](uint64_t lower, uint64_t upper)
      {
        DBG(ingest)
          << "event source @" << id() << " received id range from tracker: "
          << '[' << lower << ',' << upper << ')';

        next_id_ = lower;
        last_id_ = upper;

        send(self, atom("extract"), static_cast<size_t>(upper - lower));
      },
      after(std::chrono::seconds(10)) >> [=]
      {
        LOG(error, ingest)
          << "event source @" << id()
          << " did not receive new id range from tracker"
          << " after 10 seconds";
      });
}

void event_source::segmentize(ze::event const& e)
{
  auto n = writer_ << e;
  if (n % max_events_per_chunk_ != 0)
    return;

  writer_.flush_chunk();

  if (writer_.bytes() - writer_bytes_at_last_rotate_ < max_segment_size_)
    return;

  writer_bytes_at_last_rotate_ = writer_.bytes();

  ship_segment();
}

void event_source::ship_segment()
{
  DBG(ingest)
    << "event source @" << id()
    << " ships segment " << segment_.id()
    << " to ingestor @" << ingestor_->id();

  send(ingestor_, std::move(segment_));
  segment_ = segment();
}

} // namespace vast
