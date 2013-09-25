#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/uuid.h"

namespace vast {

/// The event index.
class index : public actor<index>
{
public:
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index(path directory);

  void act();
  char const* description() const;

private:
  void load();

  path dir_;
  cppa::actor_ptr active_;
  std::unordered_map<uuid, cppa::actor_ptr> partitions_;
};

} // namespace vast

#endif
