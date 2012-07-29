#ifndef VAST_SOURCE_BROCCOLI_H
#define VAST_SOURCE_BROCCOLI_H

#include <mutex>
#include <string>
#include <cppa/cppa.hpp>
#include "vast/event_source.h"
#include "vast/comm/broccoli.h"
#include "vast/comm/server.h"

namespace vast {
namespace source {

// TODO: Either make this a synchronous sink and inherit from
// vast::event_source or provide a separate async_source class that this
// source can inherit from.

/// Receives events from the external world.
class broccoli : public cppa::sb_actor<broccoli>
{
  friend class cppa::sb_actor<broccoli>;

public:
  /// Spawns a Broccoli event source.
  /// @param ingestor The ingestor actor.
  /// @param tracker The event ID tracker.
  broccoli(cppa::actor_ptr ingestor, cppa::actor_ptr tracker);

private:
  /// Adds an event name to the list of events to subscribe to.
  /// @param event The name of the event to subscribe to.
  void subscribe(std::string event);

  /// Starts listening for Broccoli connections at a given endpoint.
  /// @param host The address or hostname where to listen.
  /// @param port The TCP port number to bind to.
  void start_server(std::string const& host, unsigned port);

  /// Stops the TCP server.
  void stop_server();

  /// Removes a connection after an error or a remote disconnect.
  /// @param session The Broccoli session
  void disconnect(std::shared_ptr<comm::broccoli> const& session);

  comm::server server_;
  comm::broccoli::error_handler error_handler_;
  std::vector<std::string> event_names_;
  std::vector<std::shared_ptr<comm::broccoli>> broccolis_;
  std::mutex mutex_;

  cppa::actor_ptr segmentizer_;
  cppa::behavior init_state;
};

} // namespace source
} // namespace vast

#endif
