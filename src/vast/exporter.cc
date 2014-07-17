#include <caf/all.hpp>

#include "vast/event.h"
#include "vast/exporter.h"

#include "vast/sink/bro.h"
#include "vast/sink/json.h"

using namespace caf;

namespace vast {

message_handler exporter::act()
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
    [=](down_msg const&)
    {
      VAST_LOG_ACTOR_ERROR("got DOWN from " << last_sender());
      for (auto& s : sinks_)
        if (s == last_sender())
        {
          sinks_.erase(s);
          break;
        }
    },
    on(atom("add"), "bro", arg_match) >> [=](std::string const& out)
    {
      VAST_LOG_ACTOR_DEBUG("registers new bro sink");
      sinks_.insert(spawn<sink::bro, monitored>(path{out}));
    },
    on(atom("add"), "json", arg_match) >> [=](std::string const& out)
    {
      VAST_LOG_ACTOR_DEBUG("registers new JSON sink");

      auto p = path{out};
      if (out != "-")
      {
        p = p.complete();
        if (! exists(p.parent()))
        {
          auto t = mkdir(p.parent());
          if (! t)
          {
            VAST_LOG_ACTOR_ERROR("failed to create directory: " << p.parent());
            quit(exit::error);
            return;
          }
        }
      }

      sinks_.insert(spawn<sink::json, monitored>(std::move(p)));
    },
    on(atom("add"), any_vals) >> [=]
    {
      VAST_LOG_ACTOR_ERROR("got invalid sink type");
      quit(exit::error);
    },
    on(atom("limit"), arg_match) >> [=](uint64_t max)
    {
      VAST_LOG_ACTOR_DEBUG("caps event export at " << max << " events");

      if (processed_ < max)
        limit_ = max;
      else
        VAST_LOG_ACTOR_ERROR("ignores new limit of " << max <<
                             ", already processed " << processed_ << " events");
    },
    [=](event const&)
    {
      for (auto& s : sinks_)
        forward_to(s);

      if (++processed_ == limit_)
      {
        VAST_LOG_ACTOR_DEBUG("reached maximum event limit: " << limit_);
        quit(exit::done);
      }
    }
  };
}

std::string exporter::describe() const
{
  return "exporter";
}

} // namespace vast
