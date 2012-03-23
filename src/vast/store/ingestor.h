#ifndef VAST_STORE_INGESTOR_H
#define VAST_STORE_INGESTOR_H

#include <mutex>
#include <string>
#include "vast/comm/broccoli.h"
#include "vast/comm/server.h"

namespace vast {
namespace store {

/// Ingests events from the external world.
class ingestor
{
    ingestor(ingestor const&) = delete;
    ingestor& operator=(ingestor const&) = delete;

public:
    /// Constructor.
    ingestor(comm::io& io);

    /// Adds an event name to the list of events to subscribe to.
    /// @param event The name of the event to subscribe to.
    void subscribe(std::string event);

    /// Starts listening for Broccoli connections at a given endpoint.
    /// @param host The address or hostname where to listen.
    /// @param port The TCP port number to bind to.
    void init(std::string const& host, unsigned port);

    /// Stops ingesting events by closing active connections.
    void stop();

private:
    /// Stores an event.
    /// @param event The event to store in the database.
    void dispatch(std::shared_ptr<ze::event> const& event);

    /// Remove a connection after an error or a remote disconnect.
    /// @param conn The connection of a Broccoli session.
    void disconnect(std::shared_ptr<comm::broccoli> const& conn);

    comm::server server_;
    std::vector<std::string> events_;
    comm::event_handler event_handler_;
    comm::broccoli::error_handler error_handler_;
    std::vector<std::shared_ptr<comm::broccoli>> broccolis_;
    std::mutex mutex_;
};

} // namespace store
} // namespace vast

#endif
