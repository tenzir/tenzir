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
                               actor receiver,
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

behavior ingestor_actor::act()
{
  trap_exit(true);

  segmentizer_ = spawn<segmentizer, monitored>(
      this, max_events_per_chunk_, max_segment_size_);

  traverse(
      dir_,
      [&](path const& p) -> bool
      {
        VAST_LOG_ACTOR_INFO("found orphaned segment: " << p.basename());
        orphaned_.insert(p.basename());
        return true;
      });

  attach_functor(
      [=](uint32_t)
      {
        receiver_ = invalid_actor;
        source_ = invalid_actor;
        segmentizer_ = invalid_actor;
      });

  auto on_backlog = [=](bool backlogged) { backlogged_ = backlogged; };

  auto on_exit = [=](exit_msg const& e)
  {
    if (source_)
      // Tell the source to exit, it will in turn propagate the exit
      // message to the sink.
      send_exit(source_, exit::stop);
    else
      // If we have no source, we just tell the segmentizer to exit.
      send_exit(segmentizer_, e.reason);
  };

  ready_ =
  {
    on_exit,
    [=](down_msg const&)
    {
      segmentizer_ = invalid_actor;
    },
    on(atom("backlog"), arg_match) >> on_backlog,
    [=](segment& s)
    {
      VAST_LOG_ACTOR_DEBUG("sends segment " << s.id());
      send(receiver_, std::move(s), this);
      become(waiting_);
    },
    after(std::chrono::seconds(0)) >> [=]
    {
      if (! segmentizer_)
        become(terminating_);
    }
  };

  waiting_ =
  {
    on_exit,
    on(atom("backlog"), arg_match) >> on_backlog,
    on(atom("ack"), arg_match) >> [=](uuid const& id)
    {
      VAST_LOG_ACTOR_DEBUG("got ack for segment " << id);

      auto i = orphaned_.find(path{to_string(id)});
      if (i != orphaned_.end())
      {
        VAST_LOG_ACTOR_INFO("submitted orphaned segment " << id);
        rm(dir_ / *i);
        orphaned_.erase(i);
      }

      become(backlogged_ ? paused_ : ready_);
    }
  };

  paused_ =
  {
    on_exit,
    on(atom("backlog"), arg_match) >> on_backlog,
    after(std::chrono::seconds(0)) >> [=]
    {
      if (! backlogged_)
        become(ready_);
    }
  };

  terminating_ =
  {
    [=](segment const& s)
    {
      if (! exists(dir_) && ! mkdir(dir_))
      {
        VAST_LOG_ACTOR_ERROR("failed to create directory " << dir_);
        return;
      }

      auto p = dir_ / path{to_string(s.id())};
      VAST_LOG_ACTOR_INFO("archives segment to " << p);

      auto t = io::archive(p, s);
      if (! t)
        VAST_LOG_ACTOR_ERROR("failed to archive " << p << ": " << t.error());
    },
    after(std::chrono::seconds(0)) >> [=]
    {
      quit(exit::done);
    }
  };

  return
  {
    on_exit,
    [=](down_msg const& d)
    {
      quit(d.reason);
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

        // FIXME: enqueue segments in the order they have been received.
        send(this, std::move(s));
      }

      become(ready_);
    },
    on(atom("ingest"), "bro2", arg_match)
      >> [=](std::string const& file, int32_t ts_field)
    {
      VAST_LOG_ACTOR_INFO("ingests " << file);

      source_ = spawn<source::bro2, detached>(segmentizer_, file, ts_field);
      source_->link_to(segmentizer_);
      send(source_, atom("batch size"), batch_size_);
      send(source_, atom("run"));

      become(ready_);
    },
    on(atom("ingest"), val<std::string>, arg_match) >> [=](std::string const&)
    {
      VAST_LOG_ACTOR_ERROR("got invalid ingestion file type");
      quit(exit::error);
    },
  };
}

char const* ingestor_actor::describe() const
{
  return "ingestor";
}

} // namespace vast
