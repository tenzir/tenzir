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

    /// Starts event ingestion at a given endpoint.
    /// @param host The address or hostname where to listen.
    /// @param port The TCP port number to bind to.
    void init(std::string const& host, unsigned port);

private:
    /// Stores an event.
    /// @param event The event to store in the database.
    void dispatch(std::shared_ptr<ze::event> const& event);

    /// Remove a connection after an error or a remote disconnect.
    /// @param conn The connection of a Broccoli session.
    void disconnect(comm::connection_ptr const& conn);

    comm::server server_;
    comm::event_handler event_handler_;
    comm::conn_handler error_handler_;
    std::vector<std::shared_ptr<comm::broccoli>> broccolis_;
    std::mutex mutex_;
};

} // namespace store
} // namespace vast

#endif
