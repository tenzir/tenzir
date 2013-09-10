#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/uuid.h"

namespace vast {

/// The event index.
class index : public cppa::event_based_actor
{
public:
  /// Spawns the index.
  /// @param directory The root directory of the index.
  index(path directory);

  /// Implements `event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  void load();

  path dir_;
  cppa::actor_ptr active_;
  std::unordered_map<uuid, cppa::actor_ptr> partitions_;
};

} // namespace vast

#endif
