#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/fwd.h"
#include "vast/string.h"

namespace vast {

/// A horizontal partition of the index.
class partition
{
public:
  /// Constructs a partition.
  /// @param dir The absolute path of the partition.
  partition(path dir);

  /// Retrieves the directory of the partition.
  /// @returns The absolute path to the partition directory.
  path const& dir() const;

  /// Retrieves the coverage bitmap.
  bitstream const& coverage() const;

  /// Loads the partition meta index from the filesystem.
  void load();

  /// Saves the partition meta index to the filesystem.
  void save();

  /// Updates the partition meta data for a given event range.
  void update(event_id base, size_t n);

private:
  path dir_;
  bitstream coverage_;
};

struct partition_actor : actor<partition_actor>
{
  partition_actor(path dir, size_t batch_size);

  void act();
  char const* description() const;

  partition partition_;
  size_t batch_size_;
  std::unordered_map<string, cppa::actor_ptr> event_arg_indexes_;
  cppa::actor_ptr event_meta_index_;
};

} // namespace vast

#endif
