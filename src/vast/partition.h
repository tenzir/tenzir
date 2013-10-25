#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/fwd.h"
#include "vast/time.h"
#include "vast/string.h"

namespace vast {

/// A horizontal partition of the index.
class partition
{
public:
  /// Checks whether an expression looks at event meta data or not.
  static bool is_meta_query(expr::ast const& ast);

  /// Constructs a partition.
  /// @param dir The absolute path of the partition.
  partition(path dir);

  /// Retrieves the directory of the partition.
  /// @returns The absolute path to the partition directory.
  path const& dir() const;

  /// Retrieves the time point of last modification.
  time_point last_modified() const;

  /// Retrieves the coverage bitmap.
  bitstream const& coverage() const;

  /// Loads the partition meta data from the filesystem.
  void load();

  /// Saves the partition meta data to the filesystem.
  void save();

  /// Updates the partition meta data for a given event range.
  void update(event_id base, size_t n);

private:
  path dir_;
  time_point last_modified_;
  bitstream coverage_;
};

struct partition_actor : actor<partition_actor>
{
  partition_actor(path dir);

  void act();
  char const* description() const;

  partition partition_;
  std::map<string, cppa::actor_ptr> event_arg_indexes_;
  cppa::actor_ptr event_meta_index_;
};

} // namespace vast

#endif
