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
#include "vast/actor/basic_state.h"
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
struct index {
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

  struct state : basic_state {
    state(local_actor* self);

    path dir;
    accountant::type accountant;
    std::map<expression, query_state> queries;
    std::unordered_map<uuid, partition_state> partitions;
    std::list<schedule_state> schedule;
    util::cache<uuid, actor, util::mru> passive;
    std::vector<std::pair<uuid, actor>> active;
    size_t next_active = 0;
  };

  /// Spawns the index.
  /// @param dir The directory of the index.
  /// @param max_events The maximum number of events per partition.
  /// @param passive_parts The maximum number of passive partitions in memory.
  /// @param active_parts The number of active partitions to hold in memory.
  /// @pre `passive_parts > 0 && active_parts > 0`
  static behavior make(stateful_actor<state>* self, path const& dir,
                       size_t max_events, size_t passive_parts,
                       size_t active_parts);
};

} // namespace vast

#endif
