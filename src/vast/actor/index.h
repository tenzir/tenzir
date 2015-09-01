#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include <list>
#include <map>
#include <unordered_map>

#include "vast/bitstream.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/uuid.h"
#include "vast/time.h"
#include "vast/actor/actor.h"
#include "vast/actor/accountant.h"
#include "vast/util/cache.h"
#include "vast/util/flat_set.h"

namespace vast {

/// Indexes chunks by scaling horizontally over multiple partitions.
///
/// The index consists of multiple partitions. A partition loaded into memory is
/// either *active* or *passive*. An active partition can still receive chunks
/// whereas a passive partition is a sealed entity used only during querying.
/// On startup, it will scan all existing partitions on the filesystem and load
/// the k-most recent partitions into the active set, where k is configurable
/// parameter.
///
/// Arriving chunks get load-balanced across the set of active partitions. If a
/// partition becomes full, it will get evicted and replaced with a new one.
///
/// A query expression always comes with a sink actor receiving the hits. The
/// sink will receive messages in the following order:
///
///   (1) A task representing the progress of the evaluation
///   (2) Optionally a series of hits
///   (3) A DONE atom
///
/// After receiving the DONE atom the sink will not receive any further hits.
/// This sequence applies both to continuous and historical queries.
struct index : public flow_controlled_actor {
  // FIXME: only propagate overload upstream if *all* partitions are
  // overloaded. This requires tracking the set of overloaded partitions
  // manually.

  using bitstream_type = default_bitstream;

  struct schedule_state {
    uuid part;
    util::flat_set<expression> queries;
  };

  struct partition_state {
    uint64_t events = 0;
    time::point last_modified;
    time::point from = time::duration{};
    time::point to = time::duration{};
  };

  struct continuous_query_state {
    bitstream_type hits;
    actor task;
  };

  struct historical_query_state {
    bitstream_type hits;
    actor task;
    std::map<actor_addr, uuid> parts;
  };

  struct query_state {
    optional<continuous_query_state> cont;
    optional<historical_query_state> hist;
    util::flat_set<actor> subscribers;
  };

  /// Spawns the index.
  /// @param dir The directory of the index.
  /// @param max_events The maximum number of events per partition.
  /// @param passive_parts The maximum number of passive partitions to hold in
  ///                      memory.
  /// @param active_parts The number of active partitions to hold in memory.
  /// @pre `passive_parts > 0 && active_parts > 0`
  index(path const& dir, size_t max_events, size_t passive_parts,
        size_t active_parts);

  void on_exit() override;
  behavior make_behavior() override;

  /// Dispatches a query for a partition either by relaying it directly if
  /// active or enqueing it into partition queue.
  /// @param part The partition to query with *expr*.
  /// @param expr The query to look for in *part*.
  /// @returns The partition actor for *part* if *expr* can be scheduled.
  optional<actor> dispatch(uuid const& part, expression const& expr);

  /// Consolidates a query which has previously been dispatched.
  /// @param part The partition of *expr*.
  /// @param expr The query which has finished with *part*.
  /// @pre The combination of *part* and *expr* must have been dispatched.
  void consolidate(uuid const& part, expression const& expr);

  void flush();

  path dir_;
  size_t max_events_per_partition_;
  accountant::actor_type accountant_;
  std::map<expression, query_state> queries_;
  std::unordered_map<uuid, partition_state> partitions_;
  std::list<schedule_state> schedule_;
  util::cache<uuid, actor, util::mru> passive_;
  std::vector<std::pair<uuid, actor>> active_;
  size_t next_active_ = 0;
};

} // namespace vast

#endif
