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
  LOG(verbose, ingest) << "spawning ingestor @" << id();

  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size,
                                               size_t batch_size)
      {
        max_events_per_chunk_ = max_events_per_chunk;
        max_segment_size_ = max_segment_size;
        batch_size_ = batch_size;
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
        monitor(source);
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
          send(source, atom("extract"), batch_size_);
      },
      on(atom("source"), atom("ack"), arg_match) >> [=](size_t /* events */)
      {
        reply(atom("extract"), batch_size_);
      },
      on_arg_match >> [=](segment const& s)
      {
        DBG(ingest) << "ingestor @" << id()
          << " relays segment " << s.id()
          << " to archive @" << archive_->id()
          << " and index @" << index_->id();

        index_ << last_dequeued();
        archive_ << last_dequeued();

        assert(inflight_.find(s.id()) == inflight_.end());
        inflight_.emplace(s.id(), 2);

        reply(atom("segment"), atom("ack"), s.id());
      },
      on(atom("segment"), atom("ack"), arg_match) >> [=](ze::uuid const& uuid)
      {
        // Both archive and index send an ack.
        LOG(verbose, ingest)
          << "ingestor @" << id() 
          << " received segment ack from @" << last_sender()->id()
          << " for " << uuid;

        auto i = inflight_.find(uuid);
        assert(i != inflight_.end() && i->second > 0);
        if (i->second == 1)
          inflight_.erase(i);
        else
          --i->second;

        if (sources_.empty() && inflight_.empty())
          shutdown();
      },
      on(atom("shutdown")) >> [=]
      {
        if (broccoli_)
          broccoli_ << last_dequeued();
        if (sources_.empty() && inflight_.empty())
          shutdown();
        for (auto source : sources_)
          source << last_dequeued();
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t reason)
      {
        auto i = std::find(sources_.begin(), sources_.end(), last_sender());
        assert(i != sources_.end());
        sources_.erase(i);

        if (sources_.empty() && inflight_.empty())
          shutdown();
      });
}

void ingestor::shutdown()
{
  quit();
  LOG(verbose, ingest) << "ingestor @" << id() << " terminated";
}

} // namespace vast
