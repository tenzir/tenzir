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
  /// @param upstream The upstream actor receiving event batches.
  /// @param tracker The event ID tracker.
  broccoli(cppa::actor_ptr upstream, size_t batch_size
           std::string const& host, unsigned port);

  std::set<std::string> event_names_;
  std::set<cppa::actor_ptr> broccolis_;
  cppa::actor_ptr server_;
  cppa::behavior operating_;
};

} // namespace source
} // namespace vast

#endif
