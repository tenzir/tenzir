#include "vast/actor/exporter.h"

#include <caf/all.hpp>
#include "vast/event.h"

using namespace caf;

namespace vast {

void exporter::at(down_msg const& msg)
{
  for (auto& s : sinks_)
    if (s == msg.source)
    {
      sinks_.erase(s);
      break;
    }

  if (sinks_.empty())
    quit(msg.reason);
}

message_handler exporter::make_handler()
{
  attach_functor(
      [=](uint32_t reason)
      {
        for (auto& s : sinks_)
          send_exit(s, reason);

        sinks_.clear();
      });

  return
  {
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
        VAST_ERROR(this, "ignores new limit of", max <<
                   ", already processed", processed_, " events");
    },
    [=](event const&)
    {
      auto sender = actor_cast<actor>(last_sender());
      for (auto& s : sinks_)
        send_as(sender, s, last_dequeued());
      if (++processed_ == limit_)
      {
        VAST_DEBUG(this, "reached maximum event limit:", limit_);
        quit(exit::done);
      }
    },
    [=](progress_atom, double progress)
    {
      VAST_DEBUG(this, "got query progress:", size_t(progress * 100) << "%");
    },
    [=](done_atom, time::duration runtime)
    {
      VAST_DEBUG(this, "completed query in", runtime);
    }
  };
}

std::string exporter::name() const
{
  return "exporter";
}

} // namespace vast
