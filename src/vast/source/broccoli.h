#ifndef VAST_SOURCE_BROCCOLI_H
#define VAST_SOURCE_BROCCOLI_H

#include <string>
#include <set>
#include "vast/source/asynchronous.h"

namespace vast {
namespace source {

/// A Broccoli event source.
struct broccoli : asynchronous, actor<broccoli>
{
  /// Spawns a Broccoli event source.
  /// @param ingestor The ingestor actor.
  /// @param tracker The event ID tracker.
  broccoli(actor_ptr receiver, size_t batch_size);

  std::set<std::string> event_names_;
  std::set<actor_ptr> broccolis_;
  actor_ptr server_;
  behavior operating_;
};

} // namespace source
} // namespace vast

#endif
