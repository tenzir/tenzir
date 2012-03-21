#include "store/ingestor.h"

#include <algorithm>
#include <ze/event.h>
#include "comm/connection.h"
#include "comm/io.h"
#include "util/logger.h"

namespace vast {
namespace store {

ingestor::ingestor(comm::io& io)
  : server_(io.service())
  , event_handler_([&](std::shared_ptr<ze::event> const& e) { dispatch(e); })
  , error_handler_([&](comm::connection_ptr const& c) { disconnect(c); })
{
}

void ingestor::init(std::string const& host, unsigned port)
{
    server_.bind(
        host,
        port,
        [&](comm::connection_ptr const& conn)
        {
            comm::broccoli broccoli(conn, event_handler_);
            broccoli.subscribe("*");
            broccoli.run(error_handler_);

            std::lock_guard<std::mutex> lock(mutex_);
            broccolis_.push_back(std::move(broccoli));
        });
}

void ingestor::dispatch(std::shared_ptr<ze::event> const& event)
{
    LOG(debug, store) << "ingesting new event";
    LOG(debug, store) << *event;
    // TODO: Send the event down-the-line for stream querying & storing.
}

void ingestor::disconnect(comm::connection_ptr const& conn)
{
    LOG(debug, store) << "disconnecting " << conn;

    std::lock_guard<std::mutex> lock(mutex_);

    auto end = std::remove_if(
        broccolis_.begin(),
        broccolis_.end(),
        [&conn](comm::broccoli const& broccoli)
        {
            return conn == broccoli.connection();
        });

    broccolis_.erase(end, broccolis_.end());
}

} // namespace store
} // namespace vast
