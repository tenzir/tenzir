#include "vast/actor/exporter.h"

#include <caf/all.hpp>
#include "vast/event.h"

using namespace caf;

namespace vast {

exporter::exporter()
  : default_actor{"exporter"}
{
}

void exporter::on_exit()
{
  sinks_.clear();
}

behavior exporter::make_behavior()
{
  trap_exit(true);
  return
  {
    [=](exit_msg const& msg)
    {
      quit(msg.reason);
      for (auto& s : sinks_)
        send_exit(s, msg.reason);
    },
    [=](down_msg const& msg)
    {
      for (auto& s : sinks_)
        if (s == msg.source)
        {
          sinks_.erase(s);
          break;
        }
      if (sinks_.empty())
        quit(msg.reason);
    },
    [=](add_atom, actor const& snk)
    {
      monitor(snk);
      sinks_.insert(snk);
    },
    [=](limit_atom, uint64_t max)
    {
      VAST_DEBUG(this, "caps event export at", max, "events");

      if (processed_ < max)
        limit_ = max;
      else
        VAST_WARN(this, "ignores new limit of", max <<
                  ", already processed", processed_, " events");
    },
    [=](event const&)
    {
      auto sender = actor_cast<actor>(current_sender());
      for (auto& s : sinks_)
        send_as(sender, s, current_message());
      if (++processed_ == limit_)
      {
        VAST_DEBUG(this, "reached maximum event limit:", limit_);
        quit(exit::done);
        for (auto& s : sinks_)
          send_exit(s, exit::done);
      }
    },
    [=](progress_atom, double progress, uint64_t total_hits)
    {
      VAST_VERBOSE(this, "got query progress:",
                   total_hits, "hits (" <<
                   size_t(progress * 100) << "%)");
    },
    [=](done_atom, time::duration runtime)
    {
      VAST_VERBOSE(this, "got DONE from query which took" << runtime);
      quit(exit::done);
      for (auto& s : sinks_)
        send_exit(s, exit::done);
    },
    catch_unexpected()
  };
}

} // namespace vast
