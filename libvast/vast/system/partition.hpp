#ifndef VAST_SYSTEM_PARTITION_HPP
#define VAST_SYSTEM_PARTITION_HPP

#include <map>

#include <caf/actor.hpp>

#include "vast/aliases.hpp"
#include "vast/bitmap.hpp"
#include "vast/detail/flat_set.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/schema.hpp"

#include "vast/system/accountant.hpp"

namespace vast {
namespace system {

struct predicate_state {
  caf::actor task;
  bitmap hits;
  detail::flat_set<event_id> cache;
  detail::flat_set<expression const*> queries;
};

struct partition_query_state {
  caf::actor task;
  bitmap hits;
};

struct partition_state {
  caf::actor proxy;
  accountant_type accountant;
  vast::schema schema;
  size_t pending_events = 0;
  std::multimap<event_id, caf::actor> indexers;
  std::map<expression, partition_query_state> queries;
  std::map<predicate, predicate_state> predicates;
  const char* name = "partition";
};

/// A horizontal partition of the INDEX.
/// For each event batch, PARTITION spawns one event indexer per
/// type occurring in the batch and forwards to them the events.
/// @param dir The directory where to store this partition on the file system.
/// @param sink The actor receiving results of this partition.
/// @pre `sink != invalid_actor`
caf::behavior partition(caf::stateful_actor<partition_state>* self, path dir,
                        caf::actor sink);

} // namespace system
} // namespace vast

#endif
