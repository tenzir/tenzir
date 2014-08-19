#ifndef VAST_PARTITION_H
#define VAST_PARTITION_H

#include <queue>
#include "vast/actor.h"
#include "vast/chunk.h"
#include "vast/bitmap_indexer.h"
#include "vast/file_system.h"
#include "vast/schema.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/util/result.h"

namespace vast {

/// A horizontal partition of the index.
class partition
{
public:
  static path const part_meta_file;

  struct meta_data : util::equality_comparable<meta_data>
  {
    meta_data() = default;
    meta_data(uuid id);

    void update(chunk const& chk);

    uuid id;
    time_point first_event = time_range{};
    time_point last_event = time_range{};
    time_point last_modified = now();

    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// Constructs a partition.
  /// @id The UUID to use for this partition.
  partition(uuid id);

  /// Updates the partition meta data with an indexed chunk.
  void update(chunk const& chk);

  /// Retrieves the partition meta data.
  meta_data const& meta() const;

private:
  friend access;

  void serialize(serializer& sink) const;
  void deserialize(deserializer& source);

  meta_data meta_;
};

class partition_actor : public actor_base
{
public:
  partition_actor(path dir, size_t batch_size, uuid id = uuid::random());

  caf::message_handler act() final;
  std::string describe() const final;

private:
  struct statistics
  {
    uint64_t backlog = 0;         // Number of outstanding batches.
    uint64_t value_total = 0;     // Total values indexed.
    uint64_t value_rate = 0;      // Last indexing rate (values/sec).
    uint64_t value_rate_mean = 0; // Mean indexing rate (values/sec).
  };

  struct dispatcher;

  caf::actor load_time_indexer();
  caf::actor load_name_indexer();
  trial<caf::actor> load_data_indexer(type const& et, type const& t,
                                      offset const& o);

  path dir_;
  bool updated_ = false;
  size_t batch_size_;
  uint64_t max_backlog_ = 0;
  uint32_t exit_reason_ = 0;
  partition partition_;
  schema schema_;
  std::unordered_map<path, caf::actor> indexers_;
  std::unordered_map<caf::actor_addr, statistics> stats_;
  std::queue<chunk> chunks_;
  caf::actor dechunkifier_;
};

} // namespace vast

#endif
