#include "vast/comm/bro_event_source.h"

#include <algorithm>
#include <ze/event.h>
#include "vast/comm/connection.h"
#include "vast/util/logger.h"

namespace vast {
namespace comm {

bro_event_source::bro_event_source(cppa::actor_ptr upstream)
  : error_handler_([&](std::shared_ptr<broccoli> bro) { disconnect(bro); })
{
  LOG(verbose, core) << "spawning bro event source @" << id();
  using namespace cppa;
  init_state = (
      on(atom("subscribe"), arg_match) >> [=](std::string const& event)
      {
        LOG(verbose, comm)
          << "bro event source @" << id() << " subscribes to event " << event;
        subscribe(event);
      },
      on(atom("bind"), arg_match) >> [=](std::string const& host, unsigned port)
      {
        start_server(host, port, upstream);
      },
      on(atom("shutdown")) >> [=]()
      {
        stop_server();
        self->quit();
        LOG(verbose, comm) << "bro event source @" << id() << " terminated";
      });
}

void bro_event_source::subscribe(std::string event)
{
  auto i = std::lower_bound(event_names_.begin(), event_names_.end(), event);
  if (i == event_names_.end())
    event_names_.push_back(std::move(event));
  else if (event < *i)
    event_names_.insert(i, std::move(event));
}

void bro_event_source::start_server(std::string const& host, unsigned port,
                                    cppa::actor_ptr sink)
{
  server_.start(
      host,
      port,
      [=](std::shared_ptr<connection> const& conn)
      {
        auto bro = std::make_shared<broccoli>(
            conn,
            [=](ze::event event)
            {
              // TODO: send events in batches (chunks or segments).
              send(sink, std::move(event));
            });

        for (auto const& event : event_names_)
          bro->subscribe(event);

        bro->run(error_handler_);

        std::lock_guard<std::mutex> lock(mutex_);
        broccolis_.push_back(bro);
      });
}

void bro_event_source::stop_server()
{
  server_.stop();
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto const& broccoli : broccolis_)
    broccoli->stop();

  broccolis_.clear();
}

void bro_event_source::disconnect(std::shared_ptr<broccoli> const& session)
{
  std::lock_guard<std::mutex> lock(mutex_);
  broccolis_.erase(std::remove(broccolis_.begin(), broccolis_.end(), session),
                   broccolis_.end());
}

} // namespace comm
} // namespace vast
