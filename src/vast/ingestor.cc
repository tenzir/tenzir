#include "vast/ingestor.h"

#include "vast/exception.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/sink/segmentizer.h"
#include "vast/source/file.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/source/broccoli.h"
#endif

namespace vast {

using namespace cppa;

ingestor::ingestor(actor_ptr tracker,
                   actor_ptr archive,
                   actor_ptr index,
                   size_t max_events_per_chunk,
                   size_t max_segment_size,
                   size_t batch_size)
  : tracker_(tracker),
    archive_(archive),
    index_(index),
    max_events_per_chunk_(max_events_per_chunk),
    max_segment_size_(max_segment_size),
    batch_size_(batch_size)
{
  VAST_LOG_VERBOSE("spawning ingestor @" << id());
  chaining(false);
  operating_ = (
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        VAST_LOG_DEBUG("ingestor @" << id() <<
                       " received DOWN from @" << last_sender()->id());

        auto i = sinks_.find(last_sender());
        assert(i != sinks_.end());
        sinks_.erase(i);

        if (sinks_.empty() && inflight_.empty())
          shutdown();
      },
      on(atom("kill")) >> [=]
      {
        if (sinks_.empty() && inflight_.empty())
          shutdown();
        for (auto& pair : sinks_)
          pair.first << last_dequeued();
      },
#ifdef VAST_HAVE_BROCCOLI
      on(atom("ingest"), atom("broccoli"), arg_match) >>
        [=](std::string const& host, unsigned port,
            std::vector<std::string> const& events)
      {
        auto broccoli = spawn<source::broccoli>(host, port);
        init_source(broccoli);
        send(broccoli, atom("subscribe"), events);
        send(broccoli, atom("run"));
      },
#endif
      on(atom("ingest"), "bro15conn", arg_match) >> [=](std::string const& file)
      {
        auto src = spawn<source::bro15conn, detached>(file);
        init_source(src);
      },
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& file)
      {
        VAST_LOG_DEBUG("ingestor @" << id() <<
                       " spawns Bro 2 source with " << file);
        auto src = spawn<source::bro2, detached>(file);
        init_source(src);
      },
      on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      {
        VAST_LOG_ERROR("invalid ingestion file type");
      },
      on(atom("extract")) >> [=]
      {
        delayed_send(
            self,
            std::chrono::seconds(2),
            atom("statistics"), atom("print"), size_t(0));
      },
      on(atom("statistics"), arg_match) >> [=](size_t rate)
      {
        assert(sinks_.find(last_sender()) != sinks_.end());
        sinks_[last_sender()] = rate;
      },
      on(atom("statistics"), atom("print"), arg_match) >> [=](size_t last)
      {
        size_t sum = 0;
        for (auto& pair : sinks_)
          sum += pair.second;

        if (sum != last)
          VAST_LOG_INFO("ingestor @" << id() <<
                        " ingests at rate " << sum << " events/sec");

        if (! sinks_.empty())
          delayed_send(
              self,
              std::chrono::seconds(1),
              atom("statistics"), atom("print"), sum);
      },
      on_arg_match >> [=](segment const& s)
      {
        VAST_LOG_DEBUG("ingestor @" << id() <<
                       " relays segment " << s.id() <<
                       " to archive @" << archive_->id() <<
                       " and index @" << index_->id());

        index_ << last_dequeued();
        archive_ << last_dequeued();

        assert(inflight_.find(s.id()) == inflight_.end());
        inflight_.emplace(s.id(), 2);
      },
      on(atom("segment"), atom("ack"), arg_match) >> [=](uuid const& uid)
      {
        // Both archive and index send an ack.
        VAST_LOG_VERBOSE(
            "ingestor @" << id() <<
            " received segment ack from @" << last_sender()->id() <<
            " for " << uid);

        auto i = inflight_.find(uid);
        assert(i != inflight_.end() && i->second > 0);
        if (i->second == 1)
          inflight_.erase(i);
        else
          --i->second;

        if (sinks_.empty() && inflight_.empty())
          shutdown();
      });
}

void ingestor::init()
{
  become(operating_);
}

void ingestor::shutdown()
{
  self->quit();
  VAST_LOG_VERBOSE("ingestor @" << id() << " terminated");
}

void ingestor::init_source(actor_ptr src)
{
  VAST_LOG_VERBOSE("ingestor @" << id() <<
                   " spawns segmentizer for source @" << src->id());

  auto snk = spawn<sink::segmentizer>(self, max_events_per_chunk_,
                                    max_segment_size_);
  send(src, atom("init"), snk, batch_size_);

  src->link_to(snk);
  snk->link_to(src);
  self->monitor(snk);

  sinks_.emplace(std::move(snk), 0);
}

} // namespace vast
