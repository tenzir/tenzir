#ifndef VAST_STORE_EVENT_SOURCE_H
#define VAST_STORE_EVENT_SOURCE_H

#include <mutex>
#include <string>
#include <cppa/cppa.hpp>
#include <vast/comm/broccoli.h>
#include <vast/comm/server.h>

namespace vast {
namespace comm {

/// Receives events from the external world.
class bro_event_source : public cppa::sb_actor<bro_event_source>
{
  friend class cppa::sb_actor<bro_event_source>;

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
  /// @param sink The actor receiving the Broccoli events.
  void start_server(std::string const& host, unsigned port, cppa::actor_ptr sink);

  /// Stops the TCP server.
  void stop_server();

  /// Removes a connection after an error or a remote disconnect.
  /// @param session The Broccoli session
  void disconnect(std::shared_ptr<broccoli> const& session);

  server server_;
  broccoli::error_handler error_handler_;
  std::vector<std::string> event_names_;
  std::vector<std::shared_ptr<broccoli>> broccolis_;
  std::mutex mutex_;

  cppa::behavior init_state;
};

} // namespace comm
} // namespace vast

#endif
