#include "vast/importer.h"

#include "vast/sink/chunkifier.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

importer::importer(path dir, uint64_t batch_size, io::compression method)
  : dir_{dir / "import"},
    compression_{method},
    batch_size_{batch_size}
{
  attach_functor(
      [=](uint32_t)
      {
        source_ = invalid_actor;
        chunkifier_ = invalid_actor;
        sinks_.clear();
      });
}

message_handler importer::make_handler()
{
  trap_exit(true);

  chunkifier_ =
    spawn<sink::chunkifier, monitored>(this, batch_size_, compression_);

  for (auto const& p : directory{dir_ / "chunks"})
  {
    VAST_LOG_ACTOR_INFO("found orphaned chunk: " << p.basename());
    orphaned_.insert(p.basename());
    ++stored_;
  }

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

  ready_ =
  {
    on_exit,
    [=](down_msg const&)
    {
      if (last_sender() == chunkifier_)
      {
        chunkifier_ = invalid_actor;
        become(terminating_);
      }
      else
      {
        auto i = std::find_if(
            sinks_.begin(),
            sinks_.end(),
            [=](caf::actor const& a) { return a == last_sender(); });

        assert(i != sinks_.end());

        size_t idx = i - sinks_.begin();
        if (current_ > idx)
          --current_;

        VAST_LOG_ACTOR_INFO("removes sink " << last_sender());
        sinks_.erase(i);

        VAST_LOG_ACTOR_VERBOSE("has " << sinks_.size() << " sinks remaining");
        if (sinks_.empty())
          become(terminating_);
      }
    },
    on(atom("submit")) >> [=]
    {
      for (auto& basename : orphaned_)
      {
        auto p = dir_ / "chunks" / basename;
        chunk chk;
        if (io::unarchive(p, chk))
        {
          rm(p);
        }
        else
        {
          VAST_LOG_ACTOR_ERROR("failed to load orphaned chunk " << basename);
          continue;
        }

        // TODO: throttle rate.
        send(this, std::move(chk));
      }
    },
    on(atom("source"), arg_match) >> [=](actor src)
    {
      source_ = src;
      source_->link_to(chunkifier_);
      send(source_, atom("sink"), chunkifier_);
      send(source_, atom("batch size"), batch_size_);
      send(source_, atom("run"));
    },
    on(atom("sink"), arg_match) >> [=](actor snk)
    {
      send(snk, flow_control::announce{this});
      sinks_.push_back(snk);
      monitor(snk);
    },
    [=](chunk const& chk)
    {
      send(sinks_[current_++], chk);
      current_ %= sinks_.size();
    },
    [=](flow_control::overload)
    {
      VAST_LOG_ACTOR_DEBUG("pauses chunk delivery");
      become(paused_);
    },
    [=](flow_control::underload)
    {
      VAST_LOG_ACTOR_DEBUG("ignores underload signal");
    },
  };

  paused_ =
  {
    on_exit,
    [=](flow_control::overload)
    {
      VAST_LOG_ACTOR_DEBUG("ignores overload signal");
    },
    [=](flow_control::underload)
    {
      VAST_LOG_ACTOR_DEBUG("resumes chunk delivery");
      become(ready_);
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

  return ready_;
}

std::string importer::name() const
{
  return "importer";
}

} // namespace vast
