#ifndef VAST_ACTOR_PARTITION_H
#define VAST_ACTOR_PARTITION_H

#include <queue>
#include "vast/chunk.h"
#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/time.h"
#include "vast/trial.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"

namespace vast {

/// A horizontal partition of the index.
class partition : public actor_mixin<partition, flow_controlled, sentinel>
{
public:
  struct meta_data : util::equality_comparable<meta_data>
  {
    uuid id;
    time_point first_event = time_range{};
    time_point last_event = time_range{};
    time_point last_modified = now();

  private:
    friend access;
    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
    friend bool operator==(meta_data const& x, meta_data const& y);
  };

  /// Spawns a partition.
  /// @param index_dir The index directory in which to create this partition.
  /// @param id The unique ID for this partition.
  /// @param batch_size The number of events to dechunkify at once.
  partition(path const& index_dir, uuid id, size_t batch_size);

  void at_down(caf::down_msg const& msg);
  void at_exit(caf::exit_msg const& msg);
  caf::message_handler make_handler();
  std::string name() const;

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

  trial<caf::actor> create_data_indexer(type const& et, type const& t,
                                        offset const& o);
  path dir_;
  uuid id_;
  bool updated_ = false;
  uint64_t batch_size_;
  uint32_t exit_reason_ = 0;
  schema schema_;
  std::unordered_map<path, caf::actor> indexers_;
  std::unordered_map<caf::actor_addr, statistics> stats_;
  std::queue<chunk> chunks_;
  caf::actor dechunkifier_;
};

} // namespace vast

#endif
