#include "vast/actor/importer.h"

#include "vast/actor/sink/chunkifier.h"
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

void importer::at(exit_msg const& msg)
{
  if (source_)
    // Tell the source to exit, it will in turn propagate the exit
    // message to the chunkifier.
    send_exit(source_, exit::stop);
  else
    // If we have no source, we just tell the chunkifier to exit.
    send_exit(chunkifier_, msg.reason);
}

void importer::at(down_msg const& msg)
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
        [&](caf::actor const& a) { return a == msg.source; });

    assert(i != sinks_.end());

    size_t idx = i - sinks_.begin();
    if (current_ > idx)
      --current_;

    VAST_INFO(this, "removes sink", msg.source);
    sinks_.erase(i);

    VAST_VERBOSE(this, "has", sinks_.size(), "sinks remaining");
    if (sinks_.empty())
      become(terminating_);
  }
}

message_handler importer::make_handler()
{
  chunkifier_ =
    spawn<sink::chunkifier, monitored>(this, batch_size_, compression_);

  for (auto& p : directory{dir_ / "chunks"})
  {
    VAST_INFO(this, "found orphaned chunk:", p.basename());
    orphaned_.insert(p.basename());
    ++stored_;
  }

  ready_ =
  {
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
          VAST_ERROR(this, "failed to load orphaned chunk", basename);
          continue;
        }

        // TODO: throttle rate.
        send(this, std::move(chk));
      }
    },
    on(atom("add"), atom("source"), arg_match) >> [=](actor const& src)
    {
      // TODO: Support multiple sources.
      VAST_DEBUG(this, "adds source", src);
      source_ = src;
      source_->link_to(chunkifier_);
      send(source_, atom("sink"), chunkifier_);
      send(source_, atom("batch size"), batch_size_);
      send(source_, atom("run"));
    },
    on(atom("add"), atom("sink"), arg_match) >> [=](actor const& snk)
    {
      VAST_DEBUG(this, "adds sink", snk);
      send(snk, flow_control::announce{this});
      sinks_.push_back(snk);
      monitor(snk);
      return make_message(atom("ok"));
    },
    [=](chunk const& chk)
    {
      assert(! sinks_.empty());
      send(sinks_[current_++], chk);
      current_ %= sinks_.size();
    },
    [=](flow_control::overload)
    {
      VAST_DEBUG(this, "pauses chunk delivery");
      become(paused_);
    },
    [=](flow_control::underload)
    {
      VAST_DEBUG(this, "ignores underload signal");
    },
  };

  paused_ =
  {
    [=](exit_msg const& msg) { at(msg); },
    [=](flow_control::overload)
    {
      VAST_DEBUG(this, "ignores overload signal");
    },
    [=](flow_control::underload)
    {
      VAST_DEBUG(this, "resumes chunk delivery");
      become(ready_);
    },
  };

  terminating_ =
  {
    [=](chunk const& chk)
    {
      if (! exists(dir_ / "chunks") && ! mkdir(dir_ / "chunks"))
      {
        VAST_ERROR(this, "failed to create chunk directory");
        return;
      }

      auto p = dir_ / "chunks" / ("chunk-" + to_string(stored_++));
      VAST_INFO(this, "archives chunk to", p);

      auto t = io::archive(p, chk);
      if (! t)
        VAST_ERROR(this, "failed to archive chunk:", t.error());
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
