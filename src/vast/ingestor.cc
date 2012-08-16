#include "vast/ingestor.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/event_source.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/source/broccoli.h"
#include "vast/source/file.h"

namespace vast {

using namespace cppa;

ingestor::ingestor(cppa::actor_ptr tracker,
                   cppa::actor_ptr archive,
                   cppa::actor_ptr index)
  : archive_(archive)
  , index_(index)
{
  // FIXME: make batch size configurable.
  size_t batch_size = 1000;

  LOG(verbose, ingest) << "spawning ingestor @" << id();

  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size)
      {
        max_events_per_chunk_ = max_events_per_chunk;
        max_segment_size_ = max_segment_size;
      },
      on(atom("ingest"), atom("broccoli"), arg_match) >>
        [=](std::string const& host, unsigned port,
            std::vector<std::string> const& events)
      {
        broccoli_ = spawn<source::broccoli>(tracker, archive_);
        send(broccoli_,
             atom("initialize"),
             max_events_per_chunk_,
             max_segment_size_);

        for (auto& e : events)
          send(broccoli_, atom("subscribe"), e);

        send(broccoli_, atom("bind"), host, port);
      },
      on(atom("ingest"), "bro15conn", arg_match) >> [=](std::string const& file)
      {
        auto source = spawn<source::bro15conn>(self, tracker, file);
        sources_.push_back(source);
        send(source, atom("initialize"), max_events_per_chunk_,
             max_segment_size_);
      },
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& file)
      {
        auto source = spawn<source::bro2>(self, tracker, file);
        sources_.push_back(source);
        send(source, atom("initialize"), max_events_per_chunk_,
             max_segment_size_);
      },
      on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      {
        LOG(error, ingest) << "invalid ingestion file type";
      },
      on(atom("extract")) >> [=]
      {
        for (auto source : sources_)
          send(source, atom("extract"), batch_size);
      },
      on(atom("source"), atom("ack"), arg_match) >> [=](size_t /* events */)
      {
        reply(atom("extract"), batch_size);
      },
      on_arg_match >> [=](segment const& s)
      {
        DBG(ingest) << "ingestor @" << id()
          << " relays segment " << s.id()
          << " to archive @" << archive_->id()
          << " and index @" << index_->id();

        index_ << last_dequeued();
        archive_ << last_dequeued();
      },
      on(atom("shutdown")) >> [=]
      {
        terminating_ = true;

        if (broccoli_)
          broccoli_ << last_dequeued();

        if (sources_.empty())
          shutdown();
        else
          for (auto source : sources_)
            source << last_dequeued();
      },
      on(atom("shutdown"), atom("ack"), arg_match) >> [=](size_t events)
      {
        total_events_ += events;
        auto i = std::find(sources_.begin(), sources_.end(), last_sender());
        assert(i != sources_.end());
        sources_.erase(i);

        LOG(verbose, ingest)
          << "ingestor @" << id()
          << " received shutdown ack from source @" << last_sender()->id();

        if (sources_.empty() && terminating_)
          shutdown();
      });
}

void ingestor::shutdown()
{
  quit();
  LOG(verbose, ingest)
      << "ingestor @" << id() << " terminated"
      << " (ingested " << total_events_ << " events)";
}

} // namespace vast
