#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/fwd.h"
#include "vast/string.h"
#include "vast/time.h"
#include "vast/uuid.h"

namespace vast {

class segment;

/// A horizontal partition of the index.
class partition
{
public:
  static path const part_meta_file;
  static path const event_meta_dir;
  static path const event_data_dir;

  struct meta_data : util::equality_comparable<meta_data>
  {
    meta_data() = default;
    meta_data(uuid id);

    void update(segment const& s);

    uuid id;
    time_point first_event = time_range{};
    time_point last_event = time_range{};
    time_point last_modified = now();
    bitstream coverage;

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// Constructs a partition.
  /// @id The UUID to use for this partition.
  partition(uuid id);

  /// Updates the partition meta data with an indexed segment.
  void update(segment const& s);

  /// Retrieves the partition meta data.
  meta_data const& meta() const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  meta_data meta_;
};

struct partition_actor : actor<partition_actor>
{
  partition_actor(path dir, size_t batch_size, uuid id = uuid::random());

  void act();
  char const* description() const;

  path dir_;
  size_t batch_size_;
  partition partition_;
  std::unordered_map<string, cppa::actor_ptr> data_indexes_;
  cppa::actor_ptr meta_index_;
};

} // namespace vast

#endif
