#include "vast/store/ingestor.h"

#include <algorithm>
#include <ze/event.h>
#include "vast/comm/connection.h"
#include "vast/comm/io.h"
#include "vast/util/logger.h"

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
            auto bro = std::make_shared<comm::broccoli>(conn, event_handler_);
            bro->subscribe("*");
            bro->run(error_handler_);

            std::lock_guard<std::mutex> lock(mutex_);
            broccolis_.push_back(bro);
        });
}

void ingestor::dispatch(std::shared_ptr<ze::event> const& event)
{
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
        [&conn](std::shared_ptr<comm::broccoli> const& broccoli)
        {
            return conn == broccoli->connection();
        });

    broccolis_.erase(end, broccolis_.end());
}

} // namespace store
} // namespace vast
