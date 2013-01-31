#include "vast/ingestor.h"

#include <ze.h>
#include "vast/exception.h"
#include "vast/receiver.h"
#include "vast/logger.h"
#include "vast/segment.h"
#include "vast/source/file.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/source/broccoli.h"
#endif

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
#ifdef VAST_HAVE_BROCCOLI
      on(atom("ingest"), atom("broccoli"), arg_match) >>
        [=](std::string const& host, unsigned port,
            std::vector<std::string> const& events)
      {
        auto recv = spawn<receiver>(self, tracker);
        auto broccoli = spawn<source::broccoli>(recv, batch_size_);
        recv->monitor(broccoli);
        send(broccoli, atom("start"), host, port);
        for (auto& event : events)
          send(broccoli, atom("subscribe"), event);
        sources_.push_back(recv);
      },
#endif
      on(atom("ingest"), "bro15conn", arg_match) >> [=](std::string const& file)
      {
        auto recv = spawn<receiver>(self, tracker);
        unleash<source::bro15conn>(recv, file);
        sources_.push_back(recv);
      },
      //on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& file)
      //{
      //  sources_.push_back(spawn<source::bro2>(self, tracker, file));
      //},
      //on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      //{
      //  LOG(error, ingest) << "invalid ingestion file type";
      //},
      on(atom("extract")) >> [=]
      {
        for (auto source : sources_)
        {
          monitor(source);
          send(source, atom("initialize"), max_events_per_chunk_,
               max_segment_size_);
          send(source, atom("extract"), batch_size_);
        }

        size_t last = 0;
        delayed_send(
            self,
            std::chrono::seconds(2),
            atom("statistics"), atom("print"), last);
      },
      on(atom("statistics"), arg_match) >> [=](size_t rate)
      {
        rates_[last_sender()] = rate;
      },
      on(atom("statistics"), atom("print"), arg_match) >> [=](size_t last)
      {
        size_t sum = 0;
        for (auto& p : rates_)
          sum += p.second;

        if (sum != last)
          LOG(info, ingest)
            << "ingestor @" << id()
            << " ingests at rate " << sum << " events/sec";

        if (! sources_.empty())
          delayed_send(
              self,
              std::chrono::seconds(1),
              atom("statistics"), atom("print"), sum);
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

        auto j = rates_.find(last_sender());
        if (j != rates_.end())
          rates_.erase(j);

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
