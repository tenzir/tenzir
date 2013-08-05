#include "vast/source/broccoli.h"

#include "vast/logger.h"
#include "vast/util/broccoli.h"

namespace vast {
namespace source {

using namespace cppa;

broccoli::broccoli(std::string const& host, unsigned port)
{
  VAST_LOG_VERBOSE("spawning broccoli source @" << id());
  impl_ = (
      on(atom("kill")) >> [=]
      {
        server_ << last_dequeued();
        quit();
        VAST_LOG_VERBOSE("broccoli source @" << id() << " terminated");
      },
      on(atom("run")) >> [=]
      {
        // TODO: Make use of the host argument.
        VAST_LOG_VERBOSE("broccoli @" << id() <<
                         "starts server at " << host << ':' << port);
        server_ = spawn<util::broccoli::server>(port, self);
        monitor(server_);
      },
      on(atom("DOWN"), arg_match) >> [=](size_t /* exit_reason */)
      {
        if (last_sender()->id() == server_->id())
        {
          VAST_LOG_WARN("broccoli source @" << id() <<
                        " received DOWN from its server @" << server_->id());
          send(self, atom("kill"));
        }
        else
        {
          VAST_LOG_WARN("unhandled DOWN from @" << last_sender()->id());
        }
      },
      on(atom("connection"), arg_match) >> [=](actor_ptr conn)
      {
        for (auto& event : event_names_)
          send(conn, atom("subscribe"), event);
        send(conn, atom("start"), self);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event)
      {
        VAST_LOG_VERBOSE("broccoli source @" << id() <<
                         " subscribes to event " << event);
        event_names_.insert(event);
      },
      on(atom("subscribe"), arg_match)
        >> [=](std::vector<std::string> const& events)
      {
        for (auto& e : events)
          send(self, atom("subscribe"), e);
      });
}

} // namespace source
} // namespace vast
