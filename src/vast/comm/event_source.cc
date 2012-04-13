#include "vast/comm/event_source.h"

#include <algorithm>
#include <ze/event.h>
#include "vast/comm/connection.h"
#include "vast/util/logger.h"

namespace vast {
namespace comm {

event_source::event_source(ze::component& c)
  : ze::publisher<>(c)
  , server_(io_.service())
  , event_handler_([&](ze::event_ptr&& event) { send(std::move(event)); })
  , error_handler_([&](std::shared_ptr<broccoli> bro) { disconnect(bro); })
{
}

void event_source::subscribe(std::string event)
{
    auto i = std::lower_bound(events_.begin(), events_.end(), event);
    if (i == events_.end())
        events_.push_back(std::move(event));
    else if (event < *i)
        events_.insert(i, std::move(event));
}

void event_source::init(std::string const& host, unsigned port)
{
    server_.bind(
        host,
        port,
        [&](connection_ptr const& conn)
        {
            auto bro = std::make_shared<broccoli>(conn, event_handler_);
            for (auto const& event : events_)
                bro->subscribe(event);

            bro->run(error_handler_);

            std::lock_guard<std::mutex> lock(mutex_);
            broccolis_.push_back(bro);
        });
}

void event_source::stop()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto const& broccoli : broccolis_)
        broccoli->stop();

    broccolis_.clear();
}

void event_source::disconnect(std::shared_ptr<broccoli> const& session)
{
    std::lock_guard<std::mutex> lock(mutex_);
    broccolis_.erase(std::remove(
            broccolis_.begin(), broccolis_.end(), session), broccolis_.end());
}

} // namespace comm
} // namespace vast
