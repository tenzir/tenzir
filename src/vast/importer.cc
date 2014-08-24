#include "vast/importer.h"

#include "vast/sink/chunkifier.h"
#include "vast/source/bro.h"
#include "vast/io/serialization.h"

#ifdef VAST_HAVE_PCAP
#include "vast/source/pcap.h"
#endif

#ifdef VAST_HAVE_BROCCOLI
#include "vast/source/broccoli.h"
#endif

namespace vast {

using namespace caf;

importer::importer(path dir, actor receiver, uint64_t batch_size)
  : dir_{dir / "import"},
    receiver_{receiver},
    batch_size_{batch_size}
{
}

message_handler importer::act()
{
  trap_exit(true);

  chunkifier_ = spawn<sink::chunkifier, monitored>(this, batch_size_);

  traverse(
      dir_ / "chunks",
      [&](path const& p) -> bool
      {
        VAST_LOG_ACTOR_INFO("found orphaned chunk: " << p.basename());
        orphaned_.insert(p.basename());
        ++stored_;
        return true;
      });

  attach_functor(
      [=](uint32_t)
      {
        receiver_ = invalid_actor;
        source_ = invalid_actor;
        chunkifier_ = invalid_actor;
      });

  auto on_exit = [=](exit_msg const& e)
  {
    if (source_)
      // Tell the source to exit, it will in turn propagate the exit
      // message to the chunkifier.
      send_exit(source_, exit::stop);
    else
      // If we have no source, we just tell the chunkifier to exit.
      send_exit(chunkifier_, e.reason);
  };

  init_ =
  {
    on_exit,
    [=](down_msg const& d)
    {
      quit(d.reason);
    },
    on(atom("submit")) >> [=]
    {
      for (auto& basename : orphaned_)
      {
        auto p = dir_ / "chunks" / basename;
        chunk chk;
        if (! io::unarchive(p, chk))
        {
          VAST_LOG_ACTOR_ERROR("failed to load orphaned chunk " << basename);
          continue;
        }

        send(this, std::move(chk));
      }

      become(ready_);
    },
    on(atom("add"), "bro", arg_match) >> [=](std::string const& file)
    {
      VAST_LOG_ACTOR_INFO("ingests bro log: " << file);

      source_ = spawn<source::bro, detached>(chunkifier_, file, -1);
      source_->link_to(chunkifier_);
      send(source_, atom("batch size"), batch_size_);
      send(source_, atom("run"));

      become(ready_);
    },
#ifdef VAST_HAVE_PCAP
    on(atom("add"), "pcap", arg_match) >> [=](std::string const& file)
    {
      VAST_LOG_ACTOR_INFO("ingests pcap: " << file);

      source_ = spawn<source::pcap, detached>(chunkifier_, file);
      source_->link_to(chunkifier_);
      send(source_, atom("batch size"), batch_size_);
      send(source_, atom("run"));

      become(ready_);
    },
#endif
    on(atom("add"), any_vals) >> [=]
    {
      VAST_LOG_ACTOR_ERROR("got invalid import file type");
      quit(exit::error);
    },
  };

  ready_ =
  {
    on_exit,
    [=](down_msg const&)
    {
      chunkifier_ = invalid_actor;
    },
    on(atom("backlog"), arg_match) >> [=](bool backlogged)
    {
      if (backlogged)
      {
        VAST_LOG_ACTOR_DEBUG("pauses chunk delivery");
        become(paused_);
      }
    },
    [=](chunk const& chk)
    {
      send(receiver_, chk, this);
    },
    after(std::chrono::seconds(0)) >> [=]
    {
      if (! chunkifier_)
        become(terminating_);
    }
  };

  paused_ =
  {
    on_exit,
    on(atom("backlog"), arg_match) >> [=](bool backlogged)
    {
      if (! backlogged)
      {
        VAST_LOG_ACTOR_DEBUG("resumes chunk delivery");
        become(ready_);
      }
    },
  };

  terminating_ =
  {
    [=](chunk const& chk)
    {
      if (! exists(dir_ / "chunks") && ! mkdir(dir_ / "chunks"))
      {
        VAST_LOG_ACTOR_ERROR("failed to create chunk directory");
        return;
      }

      auto p = dir_ / "chunks" / ("chunk-" + to_string(stored_++));
      VAST_LOG_ACTOR_INFO("archives chunk to " << p);

      auto t = io::archive(p, chk);
      if (! t)
        VAST_LOG_ACTOR_ERROR("failed to archive chunk: " << t.error());
    },
    after(std::chrono::seconds(0)) >> [=]
    {
      quit(exit::done);
    }
  };

  return init_;
}

std::string importer::describe() const
{
  return "importer";
}

} // namespace vast
