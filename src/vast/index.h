#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include "vast/actor.h"
#include "vast/file_system.h"

namespace vast {

/// The event index.
struct index_actor : actor<index_actor>
{
  /// Spawns the index.
  /// @param directory The root directory of the index.
  /// @param batch_size The number
  index_actor(path directory, size_t batch_size);

  void act();
  char const* description() const;

  path dir_;
  size_t batch_size_;
  cppa::actor_ptr active_;
  std::map<path, cppa::actor_ptr> partitions_;
};

} // namespace vast

#endif
