#include <vast/source/broccoli.h>

#include <algorithm>
#include <ze/event.h>
#include "vast/comm/connection.h"
#include "vast/logger.h"
#include "vast/segmentizer.h"

namespace vast {
namespace source {

broccoli::broccoli(cppa::actor_ptr ingestor, cppa::actor_ptr tracker)
  : error_handler_([&](std::shared_ptr<comm::broccoli> bro) { disconnect(bro); })
{
  LOG(verbose, core) << "spawning bro event source @" << id();
  using namespace cppa;
  chaining(false);
  init_state = (
      on(atom("initialize"), arg_match) >> [=](size_t max_events_per_chunk,
                                               size_t max_segment_size)
      {
        segmentizer_ = spawn<segmentizer>(max_events_per_chunk,
                                          max_segment_size);
      },
      on(atom("subscribe"), arg_match) >> [=](std::string const& event)
      {
        LOG(verbose, ingest)
          << "bro event source @" << id() << " subscribes to event " << event;

        subscribe(event);
      },
      on(atom("bind"), arg_match) >> [=](std::string const& host, unsigned port)
      {
        assert(segmentizer_);
        start_server(host, port);
      },
      on(atom("shutdown")) >> [=]()
      {
        stop_server();
        // TODO: make sure last segment get's accommodated.
        segmentizer_ << last_dequeued();
        quit();
        LOG(verbose, ingest) << "bro event source @" << id() << " terminated";
      });
}

void broccoli::subscribe(std::string event)
{
  auto i = std::lower_bound(event_names_.begin(), event_names_.end(), event);
  if (i == event_names_.end())
    event_names_.push_back(std::move(event));
  else if (event < *i)
    event_names_.insert(i, std::move(event));
}

void broccoli::start_server(std::string const& host, unsigned port)
{
  server_.start(
      host,
      port,
      [=](std::shared_ptr<comm::connection> const& conn)
      {
        auto bro = std::make_shared<comm::broccoli>(
            conn,
            [=](ze::event event)
            {
              // FIXME: assign events an ID from the tracker.
              send(segmentizer_, std::move(event));
            });

        for (auto const& event : event_names_)
          bro->subscribe(event);

        bro->run(error_handler_);

        std::lock_guard<std::mutex> lock(mutex_);
        broccolis_.push_back(bro);
      });
}

void broccoli::stop_server()
{
  server_.stop();
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto const& broccoli : broccolis_)
    broccoli->stop();

  broccolis_.clear();
}

void broccoli::disconnect(std::shared_ptr<comm::broccoli> const& session)
{
  std::lock_guard<std::mutex> lock(mutex_);
  broccolis_.erase(std::remove(broccolis_.begin(), broccolis_.end(), session),
                   broccolis_.end());
}

} // namespace source
} // namespace vast
