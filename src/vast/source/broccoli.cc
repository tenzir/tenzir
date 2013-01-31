#include "vast/source/broccoli.h"

#include <algorithm>
#include <ze.h>
#include <ze/io.h>
#include "vast/logger.h"
#include "vast/util/broccoli.h"

namespace vast {
namespace source {

using namespace cppa;

broccoli::broccoli(actor_ptr receiver, size_t batch_size)
  : asynchronous(receiver, batch_size)
{
  LOG(verbose, core) << "spawning broccoli source @" << id();
  chaining(false);
  derived_ = (
      on(atom("start"), arg_match) >> [=](std::string const& host, unsigned port)
      {
        server_ = spawn<util::broccoli::server>(port, self);
        monitor(server_);
      },
      on(atom("DOWN"), arg_match) >> [](size_t exit_reason)
      {
        LOG(error, ingest)
          << "broccoli source @" << id() 
          << " noticed termination of its server @" << server_->id();
        send(self, atom("shutdown"));
      },
      on(atom("shutdown")) >> [=]
      {
        server_ << last_dequeued();
        quit();
        LOG(verbose, ingest) << "broccoli source @" << id() << " terminated";
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
          << "broccoli source @" << id() << " subscribes to event " << event;
        event_names_.insert(event);
      },
      on_arg_match >> [=](ze::event& event)
      {
        buffer(event);
      });
}

} // namespace source
} // namespace vast
