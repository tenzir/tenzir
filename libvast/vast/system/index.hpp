#ifndef VAST_INDEX_HPP
#define VAST_INDEX_HPP

#include <list>
#include <map>
#include <unordered_map>

#include <caf/stateful_actor.hpp>

#include "vast/bitmap.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/uuid.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"

#include "vast/detail/cache.hpp"
#include "vast/detail/flat_set.hpp"

#include "vast/system/accountant.hpp"

namespace vast {
namespace system {

struct schedule_state {
  uuid part;
  detail::flat_set<expression> queries;
};

struct continuous_query_state {
  bitmap hits;
  caf::actor task;
};

struct historical_query_state {
  bitmap hits;
  caf::actor task;
  std::unordered_map<caf::actor_addr, uuid> parts;
};

struct index_query_state {
  optional<continuous_query_state> cont;
  optional<historical_query_state> hist;
  detail::flat_set<caf::actor> subscribers;
};

struct index_partition_state {
  timestamp last_modified;
  vast::schema schema;
  uint64_t events = 0;
  // Our poor-man's version of a "meta index". To be factored into a separate
  // actor in the future.
  timestamp from = timestamp::max();
  timestamp to = timestamp::min();
};

template <class Inspector>
auto inspect(Inspector& f, index_partition_state& s) {
  return f(s.last_modified, s.schema, s.events, s.from, s.to);
}

struct index_state {
  std::list<schedule_state> schedule;
  std::map<expression, index_query_state> queries;
  std::unordered_map<uuid, index_partition_state> partitions;
  caf::actor active;
  uuid active_id;
  detail::cache<uuid, caf::actor, detail::mru> passive;
  accountant_type accountant;
  path dir;
  char const* name = "index";
};

/// Indexes chunks by scaling horizontally over multiple partitions.
///
/// The index consists of multiple partitions. A partition loaded into memory is
/// either *active* or *passive*. An active partition can still receive chunks
/// whereas a passive partition is a sealed entity used only during querying.
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
///
/// @param dir The directory of the index.
/// @param max_events The maximum number of events per partition.
/// @param passive The maximum number of passive partitions in memory.
/// @pre `max_events > 0 && passive > 0`
caf::behavior index(caf::stateful_actor<index_state>* self, path const& dir,
                    size_t max_events, size_t passive);

} // namespace system
} // namespace vast

#endif
