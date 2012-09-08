#include "vast/source/broccoli.h"

#include <algorithm>
#include <ze.h>
#include <ze/io.h>
#include "vast/logger.h"
#include "vast/util/broccoli.h"

namespace vast {
namespace source {

using namespace cppa;

broccoli::broccoli(actor_ptr ingestor, actor_ptr tracker)
{
  LOG(verbose, core) << "spawning bro event source @" << id();
  chaining(false);
  init_state = (
      on(atom("start"), arg_match) >> [=](std::string const& host, unsigned port)
      {
        server_ = spawn<util::broccoli::server>(port, self);
      },
      on(atom("connection"), arg_match) >> [=](actor_ptr conn)
      {
        for (auto& event : event_names_)
          send(conn, atom("subscribe"), event);
        send(conn, atom("start"), self);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event)
      {
        LOG(verbose, ingest)
          << "bro event source @" << id() << " subscribes to event " << event;
        event_names_.insert(event);
      },
      on_arg_match >> [=](ze::event const& e)
      {
        // TODO.
        DBG(ingest) << e;
      },
      on(atom("shutdown")) >> [=]
      {
        server_ << last_dequeued();
        quit();
        LOG(verbose, ingest) << "bro event source @" << id() << " terminated";
      });
}

} // namespace source
} // namespace vast
