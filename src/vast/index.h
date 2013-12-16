#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/uuid.h"

namespace vast {

/// The event index.
struct index_actor : actor<index_actor>
{
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index_actor(path directory);

  void act();
  char const* description() const;

  path dir_;
  cppa::actor_ptr active_;
  std::unordered_map<uuid, cppa::actor_ptr> partitions_;
};

} // namespace vast

#endif
