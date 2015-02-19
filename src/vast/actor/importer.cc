#include "vast/actor/importer.h"

#include "vast/actor/sink/chunkifier.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

importer::importer(path dir, uint64_t chunk_size, io::compression method)
  : dir_{dir / "import"},
    chunk_size_{chunk_size},
    compression_{method}
{
  attach_functor([=](uint32_t)
  {
    source_ = invalid_actor;
    chunkifier_ = invalid_actor;
    accountant_ = invalid_actor;
    sinks_.clear();
  });

  terminating_ =
  {
    [=](exit_msg const& msg)
    {
      quit(msg.reason);
    },
    [=](chunk const& chk)
    {
      if (! sinks_.empty())
      {
        VAST_DEBUG(this, "relays lingering chunk with", chk.events(), "events");
        send(sinks_[current_++], chk);
        current_ %= sinks_.size();
        return;
      }
      if (! exists(dir_ / "chunks") && ! mkdir(dir_ / "chunks"))
      {
        VAST_ERROR(this, "failed to create chunk backup directory");
        return;
      }
      auto p = dir_ / "chunks" / ("chunk-" + to_string(stored_++));
      VAST_INFO(this, "archives chunk to", p);
      auto t = io::archive(p, chk);
      if (! t)
        VAST_ERROR(this, "failed to archive chunk:", t.error());
    }
  };
}

void importer::at(exit_msg const& msg)
{
  if (source_)
    // Tell the source to exit, it will in turn propagate the exit
    // message to the chunkifier because they are linked.
    send_exit(source_, exit::stop);
  else if (chunkifier_)
    // If we have no source, we just tell the chunkifier to exit.
    send_exit(chunkifier_, msg.reason);
  else
    assert(! "should never happen");
}

void importer::at(down_msg const& msg)
{
  if (last_sender() == chunkifier_)
  {
    chunkifier_ = invalid_actor;
    become(terminating_);
    send(this, exit_msg{address(), msg.reason});
    return;
  }
  for (auto s = sinks_.begin(); s != sinks_.end(); ++s)
    if (s->address() == last_sender())
    {
      VAST_VERBOSE(this, "removes sink", msg.source);
      size_t idx = s - sinks_.begin();
      if (current_ > idx)
        --current_;
      sinks_.erase(s);
      if (sinks_.empty())
      {
        become(terminating_);
        send(this, exit_msg{address(), msg.reason});
      }
      else
      {
        VAST_VERBOSE(this, "has", sinks_.size(), "sinks remaining");
      }
      break;
    }
}

message_handler importer::make_handler()
{
  chunkifier_ =
    spawn<sink::chunkifier, monitored>(this, chunk_size_, compression_);
  if (accountant_)
    send(chunkifier_, accountant_atom::value, accountant_);
  for (auto& p : directory{dir_ / "chunks"})
  {
    VAST_INFO(this, "found orphaned chunk:", p.basename());
    orphaned_.insert(p.basename());
    ++stored_;
  }
  return
  {
    [=](submit_atom)
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
        send(this, std::move(chk)); // TODO: throttle rate.
      }
    },
    [=](add_atom, source_atom, actor const& src)
    {
      // TODO: Support multiple sources.
      VAST_DEBUG(this, "adds source", src);
      register_upstream(src);
      source_ = src;
      source_->link_to(chunkifier_);
      send(source_, sink_atom::value, chunkifier_);
      send(source_, batch_atom::value, chunk_size_);
      if (accountant_)
        send(source_, accountant_atom::value, accountant_);
      send(source_, run_atom::value);
    },
    [=](add_atom, sink_atom, actor const& snk)
    {
      VAST_DEBUG(this, "adds sink", snk);
      send(snk, flow_control::announce{this});
      sinks_.push_back(snk);
      monitor(snk);
      return ok_atom::value;
    },
    [=](accountant_atom, actor const& accountant)
    {
      VAST_DEBUG(this, "registers accountant", accountant);
      accountant_ = accountant;
    },
    [=](chunk const& chk)
    {
      assert(! sinks_.empty());
      send(sinks_[current_++], chk);
      current_ %= sinks_.size();
    }
  };
}

std::string importer::name() const
{
  return "importer";
}

} // namespace vast
