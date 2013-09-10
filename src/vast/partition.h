#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include <cppa/cppa.hpp>
#include "vast/file_system.h"
#include "vast/time.h"

namespace vast {

/// A horizontal partition of the index.
class partition : public cppa::event_based_actor
{
public:
  /// Spawns a partition actor.
  /// @param dir The absolute path of the partition.
  partition(path dir);

  /// Implements `event_based_actor::init`.
  virtual void init() final;

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

private:
  path dir_;
  time_point last_modified_;
  cppa::actor_ptr meta_;
  cppa::actor_ptr type_;
};

} // namespace vast

#endif
