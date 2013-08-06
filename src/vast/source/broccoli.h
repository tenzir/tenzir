#ifndef VAST_SOURCE_BROCCOLI_H
#define VAST_SOURCE_BROCCOLI_H

#include <string>
#include <set>
#include "vast/source/asynchronous.h"

namespace vast {
namespace source {

/// A Broccoli event source.
struct broccoli : asynchronous<broccoli>
{
  /// Spawns a Broccoli event source.
  /// @param sink The sink to send events to.
  /// @param host The host to bind to.
  /// @param port The TCP port to listen on.
  broccoli(cppa::actor:ptr sink, std::string const& host, unsigned port)

  std::set<std::string> event_names_;
  std::set<cppa::actor_ptr> broccolis_;
  cppa::actor_ptr server_;
  cppa::behavior impl_;
};

} // namespace source
} // namespace vast

#endif
