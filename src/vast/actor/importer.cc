#include "vast/actor/importer.h"

#include "vast/actor/sink/chunkifier.h"
#include "vast/concept/serializable/chunk.h"
#include "vast/io/serialization.h"

namespace vast {

using namespace caf;

importer::importer(path dir, uint64_t chunk_size, io::compression method)
  : flow_controlled_actor{"importer"},
    dir_{dir / "import"},
    chunk_size_{chunk_size},
    compression_{method},
    sink_pool_{actor_pool::make(actor_pool::round_robin{})}
{
  trap_exit(true);
}

void importer::on_exit()
{
  sink_pool_ = invalid_actor;
  source_ = invalid_actor;
  chunkifier_ = invalid_actor;
  accountant_ = invalid_actor;
}

caf::behavior importer::make_behavior()
{
  monitor(sink_pool_);
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
  terminating_ =
  {
    [=](exit_msg const& msg)
    {
      quit(msg.reason);
    },
    [=](chunk const& chk)
    {
      if (sink_pool_ != invalid_actor)
      {
        VAST_DEBUG(this, "relays lingering chunk with", chk.events(),
                   "events");
        send(sink_pool_, chk);
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
  return
  {
    register_upstream_node(),
    forward_overload(),
    forward_underload(),
    [=](exit_msg const& msg)
    {
      if (downgrade_exit())
        return;
      if (source_)
        // Tell the source to exit, it will in turn propagate the exit
        // message to the chunkifier because they are linked.
        send_exit(source_, exit::stop);
      else if (chunkifier_)
        // If we have no source, we just tell the chunkifier to exit.
        send_exit(chunkifier_, msg.reason);
      else
        assert(! "should never happen");
    },
    [=](down_msg const& msg)
    {
      if (remove_upstream_node(msg.source))
        return;
      if (current_sender() == chunkifier_)
      {
        chunkifier_ = invalid_actor;
        become(terminating_);
        send(this, exit_msg{address(), msg.reason});
        return;
      }
      if (current_sender() == sink_pool_)
      {
        VAST_DEBUG(this, "begins termination");
        sink_pool_ = invalid_actor;
        become(terminating_);
        send(this, exit_msg{address(), msg.reason});
      }
    },
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
      add_upstream_node(src);
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
      send(snk, upstream_atom::value, this);
      send(sink_pool_, sys_atom::value, put_atom::value, snk);
      return ok_atom::value;
    },
    [=](accountant_atom, actor const& accountant)
    {
      VAST_DEBUG(this, "registers accountant", accountant);
      accountant_ = accountant;
    },
    [=](chunk const& chk)
    {
      send(sink_pool_, chk);
    },
    catch_unexpected()
  };
}

} // namespace vast
