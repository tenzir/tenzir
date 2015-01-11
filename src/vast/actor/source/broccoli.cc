#include "vast/actor/source/broccoli.h"

#include "vast/logger.h"
#include "vast/util/broccoli.h"

namespace vast {
namespace source {

using namespace caf;

broccoli::broccoli(actor_ptr sink, std::string const& host, unsigned port)
{
  VAST_LOG_VERBOSE("spawning broccoli source @" << id());
  impl_ = (
      on(atom("run")) >> [=]
      {
        // TODO: Make use of the host argument.
        VAST_LOG_VERBOSE("broccoli @" << id() <<
                         "starts server at " << host << ':' << port);
        server_ = spawn<util::broccoli::server, linked>(port, self);
      },
      on(atom("connection"), arg_match) >> [=](actor_ptr conn)
      {
        for (auto& event : event_names_)
          send(conn, atom("subscribe"), event);
        // TODO: Consider one segmentizer per connection as opposed to using
        // one for all connections.
        send(conn, atom("start"), sink);
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
