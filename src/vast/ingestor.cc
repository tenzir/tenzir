#include "vast/ingestor.h"

#include "vast/segment.h"
#include "vast/source/file.h"
#include "vast/io/serialization.h"

#ifdef VAST_HAVE_BROCCOLI
#include "vast/source/broccoli.h"
#endif

namespace vast {

using namespace cppa;

ingestor_actor::ingestor_actor(path dir,
                               actor_ptr receiver,
                               size_t max_events_per_chunk,
                               size_t max_segment_size,
                               uint64_t batch_size)
  : dir_{std::move(dir)},
    receiver_{receiver},
    max_events_per_chunk_{max_events_per_chunk},
    max_segment_size_{max_segment_size},
    batch_size_{batch_size}
{
}

void ingestor_actor::act()
{
  trap_exit(true);

  // FIXME: figure out why detaching the segmentizer yields a two-fold
  // performance increase in the ingestion rate.
  sink_ = spawn<segmentizer, monitored>(
      self, max_events_per_chunk_, max_segment_size_);

  become(
      on(atom("terminate"), arg_match) >> [=](uint32_t reason)
      {
        if (segments_.empty())
        {
          quit(reason);
        }
        else
        {
          VAST_LOG_ACTOR_INFO("writes un-acked segments to stable storage");

          auto segment_dir = dir_ / "ingest" / "segments";
          if (! exists(segment_dir) && ! mkdir(segment_dir))
          {
            VAST_LOG_ACTOR_ERROR("failed to create directory " << segment_dir);
          }
          else
          {
            for (auto& p : segments_)
            {
              auto const path = segment_dir / to<string>(p.first);
              VAST_LOG_ACTOR_INFO("saves " << path);
              io::archive(path, p.second);
            }
          }

          quit(exit::error);
        }
      },
      on(atom("shutdown"), arg_match) >> [=](uint32_t reason)
      {
        if (segments_.empty())
        {
          quit(reason);
        }
        else
        {
          delayed_send(self, std::chrono::seconds(10),
                       atom("terminate"), reason);

          VAST_LOG_ACTOR_INFO(
              "waits 10 seconds for " << segments_.size() << " segment ACKs");
        }
      },
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (! source_)
          send(self, atom("shutdown"), reason);
        else
          // Tell the source to exit, it will in turn propagate the exit
          // message to the sink.
          send_exit(source_, exit::stop);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        // Once we have received DOWN from the sink, the ingestor has nothing
        // else left todo and can shutdown.
        delayed_send(self, std::chrono::seconds(5), atom("shutdown"), exit::done);
      },
#ifdef VAST_HAVE_BROCCOLI
      on(atom("ingest"), atom("broccoli"), arg_match) >>
        [=](std::string const& host, unsigned port,
            std::vector<std::string> const& events)
      {
        // TODO.
      },
#endif
      on(atom("ingest"), "bro2", arg_match) >> [=](std::string const& file)
      {
        source_ = spawn<source::bro2, detached>(sink_, file);
        source_->link_to(sink_);
        send(source_, atom("batch size"), batch_size_);
        send(source_, atom("run"));
      },
      on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
      {
        VAST_LOG_ACTOR_ERROR("got invalid ingestion file type");
      },
      on_arg_match >> [=](segment& s)
      {
        VAST_LOG_ACTOR_DEBUG(
            "relays segment " << s.id() << " to " << VAST_ACTOR_ID(receiver_));

        auto cs = cow<segment>{std::move(s)};
        segments_[cs->id()] = cs;
        receiver_ << cs;
      },
      on(atom("ack"), arg_match) >> [=](uuid const& id)
      {
        VAST_LOG_ACTOR_DEBUG("got ack for segment " << id);
        auto i = segments_.find(id);
        assert(i != segments_.end());
        segments_.erase(i);
      },
      on(atom("nack"), arg_match) >> [=](uuid const& id)
      {
        VAST_LOG_ACTOR_ERROR("got nack for segment " << id);
        send(self, atom("shutdown"), exit::error);
      });
}

char const* ingestor_actor::description() const
{
  return "ingestor";
}

} // namespace vast
