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
  : dir_{dir / "ingest" / "segments"},
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

  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        VAST_LOG_ACTOR_INFO("found orphaned segment: " << p.basename());
        orphaned_.insert(p.basename());
        return true;
      });

  become(
      on(atom("shutdown"), arg_match) >> [=](uint32_t reason)
      {
        if (buffer_.empty())
        {
          quit(reason);
        }
        else if (! terminating_)
        {
          terminating_ = true;
          VAST_LOG_ACTOR_INFO("waits 30 seconds for segment ACK");
          delayed_send(self, std::chrono::seconds(30),
                       atom("shutdown"), reason);
        }
        else
        {
          VAST_LOG_ACTOR_WARN("writes un-acked segments to filesystem");

          if (! exists(dir_) && ! mkdir(dir_))
          {
            VAST_LOG_ACTOR_ERROR("failed to create directory " << dir_);
          }
          else
          {
            while (! buffer_.empty())
            {
              auto p = dir_ / path{to_string(buffer_.front()->id())};
              VAST_LOG_ACTOR_INFO("archives segment to " << p);

              auto t = io::archive(p, buffer_.front());
              if (! t)
                VAST_LOG_ACTOR_ERROR("failed to archive " << p << ": " <<
                                     t.error());

              buffer_.pop();
            }
          }

          quit(exit::error);
        }
      },
      on(atom("EXIT"), arg_match) >> [=](uint32_t reason)
      {
        if (source_)
          // Tell the source to exit, it will in turn propagate the exit
          // message to the sink.
          send_exit(source_, exit::stop);
        else
          // We have always a sink, and if the source doesn't shut it down, we
          // have to do it ourselves.
          send_exit(sink_, reason);
      },
      on(atom("DOWN"), arg_match) >> [=](uint32_t /* reason */)
      {
        // Once we have received DOWN from the sink, the ingestor has nothing
        // else left todo and can shutdown.
        send(self, atom("shutdown"), exit::done);
      },
      on(atom("submit")) >> [=]
      {
        for (auto& base : orphaned_)
        {
          auto p = dir_ / base;
          segment s;
          if (! io::unarchive(p, s))
          {
            VAST_LOG_ACTOR_ERROR("failed to load orphaned segment " << base);
            continue;
          }

          // FIXME: insert segments in the order they have been received.
          buffer_.emplace(std::move(s));
        }

        if (! buffer_.empty())
          send(self, atom("process"));
      },
      on(atom("ingest"), "bro2", arg_match)
        >> [=](std::string const& file, int32_t ts_field)
      {
        VAST_LOG_ACTOR_INFO("ingests " << file);

        source_ = spawn<source::bro2, detached>(sink_, file, ts_field);
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
        buffer_.emplace(std::move(s));

        if (buffer_.size() == 1)
          send(self, atom("process"));
      },
      on(atom("process")) >> [=]
      {
        assert(! buffer_.empty());

        any_tuple s = buffer_.front();
        if (delay_ > 0)
        {
          VAST_LOG_ACTOR_DEBUG("waits " << delay_ << " sec to send segment " <<
                               buffer_.front()->id());

          delayed_send_tuple(receiver_, std::chrono::seconds(delay_), s);
        }
        else
        {
          VAST_LOG_ACTOR_DEBUG("sends segment " << buffer_.front()->id());
          receiver_ << s;
        }
      },
      on(atom("ack"), arg_match) >> [=](uuid const& id)
      {
        VAST_LOG_ACTOR_DEBUG("got ack for segment " << id);

        if (buffer_.front()->id() != id)
        {
          auto i = orphaned_.find(path{to_string(id)});
          if (i != orphaned_.end())
          {
            VAST_LOG_ACTOR_INFO("submitted orphaned segment " << id);
            rm(dir_ / *i);
            orphaned_.erase(i);
          }
        }

        buffer_.pop();

        if (! buffer_.empty())
          send(self, atom("process"));
        else if (terminating_)
          quit(exit::done);
      },
      on(atom("delay"), arg_match) >> [=](uint64_t delay)
      {
        VAST_LOG_ACTOR_DEBUG("got delay update: " << delay);
        delay_ = delay;
      });
}

char const* ingestor_actor::description() const
{
  return "ingestor";
}

} // namespace vast
