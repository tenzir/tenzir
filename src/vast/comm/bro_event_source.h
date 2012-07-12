#ifndef VAST_STORE_EVENT_SOURCE_H
#define VAST_STORE_EVENT_SOURCE_H

#include <mutex>
#include <string>
#include <ze/vertex.h>
#include <vast/comm/broccoli.h>
#include <vast/comm/server.h>

namespace vast {
namespace comm {

/// Receives events from the external world.
class bro_event_source : cppa::sb_actor<bro_event_source>
{
public:
  /// Constructs a Bro event source.
  /// @param upstream The actor to send the received events to.
  bro_event_source(cppa::actor_ptr upstream);

private:
  /// Adds an event name to the list of events to subscribe to.
  /// @param event The name of the event to subscribe to.
  void subscribe(std::string event);

  /// Starts listening for Broccoli connections at a given endpoint.
  /// @param host The address or hostname where to listen.
  /// @param port The TCP port number to bind to.
  void bind(std::string const& host, unsigned port);

  /// Stops ingesting events by closing active connections.
  void stop();

  /// Removes a connection after an error or a remote disconnect.
  /// @param session The Broccoli session
  void disconnect(std::shared_ptr<broccoli> const& session);

  boost::asio::io_service io_service_;
  server server_;
  broccoli::error_handler error_handler_;
  std::vector<std::string> events_;
  std::vector<std::shared_ptr<broccoli>> broccolis_;
  std::mutex mutex_;
};

} // namespace comm
} // namespace vast

#endif
