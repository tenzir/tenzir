#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/time.h"
#include "vast/string.h"

namespace vast {

/// A horizontal partition of the index.
class partition : public actor<partition>
{
public:
  /// Spawns a partition actor.
  /// @param dir The absolute path of the partition.
  partition(path dir);

  /// Overrides `event_based_actor::on_exit`.
  virtual void on_exit() final;

  void act();
  char const* description() const;

private:
  path dir_;
  time_point last_modified_;
  std::map<string, cppa::actor_ptr> event_arg_indexes_;
  cppa::actor_ptr event_meta_index_;
};

} // namespace vast

#endif
