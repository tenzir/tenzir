#include "vast/comm/bro_event_source.h"

#include <algorithm>
#include <ze/event.h>
#include "vast/comm/connection.h"
#include "vast/util/logger.h"

namespace vast {
namespace comm {

bro_event_source::bro_event_source(cppa::actor_ptr upstream)
  : server_(active_io_service_)
  , error_handler_([&](std::shared_ptr<broccoli> bro) { disconnect(bro); })
{
    using namespace cppa;

    init_state = (
        on(atom("subscribe"), arg_match) >> [=](std::string const& event)
        {
          subscribe(event);
        },
        on(atom("bind"), arg_match) >> [=](std::string const& host, unsigned port)
        {
          bind(host, port, upstream);
          active_io_service_.start();
        },
        on(atom("shutdown")) >> [=]()
        {
          // Stop all Broccolis.
          stop();
          active_io_service_.stop();
          self->quit();
          LOG(verbose, comm) << "bro event source terminated";
        });
}

void bro_event_source::subscribe(std::string event)
{
  auto i = std::lower_bound(events_.begin(), events_.end(), event);
  if (i == events_.end())
    events_.push_back(std::move(event));
  else if (event < *i)
    events_.insert(i, std::move(event));
}

void bro_event_source::bind(std::string const& host, unsigned port,
                            cppa::actor_ptr sink)
{
  server_.bind(
      host,
      port,
      [=](std::shared_ptr<connection> const& conn)
      {
        auto bro = std::make_shared<broccoli>(
            conn,
            [=](ze::event event) { send(sink, std::move(event)); });

        for (auto const& event : events_)
          bro->subscribe(event);

        bro->run(error_handler_);

        std::lock_guard<std::mutex> lock(mutex_);
        broccolis_.push_back(bro);
      });
}

void bro_event_source::stop()
{
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
