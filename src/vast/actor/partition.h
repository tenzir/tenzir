#ifndef VAST_ACTOR_PARTITION_H
#define VAST_ACTOR_PARTITION_H

#include <map>
#include <set>
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/actor/accountant.h"
#include "vast/actor/basic_state.h"
#include "vast/expr/evaluator.h"

namespace vast {

/// A horizontal partition of the index.
///
/// For each event batch PARTITION receives, it spawns one EVENT_INDEXERs per
/// type occurring in the batch and forwards them the events.
struct partition {
  using bitstream_type = default_bitstream;

  struct predicate_state {
    actor task;
    bitstream_type hits;
    util::flat_set<event_id> cache;
    util::flat_set<expression const*> queries;
  };

  struct query_state {
    actor task;
    bitstream_type hits;
  };

  struct state : basic_state {
    state(local_actor* self);

    actor proxy;
    accountant::type accountant;
    vast::schema schema;
    size_t pending_events = 0;
    std::multimap<event_id, actor> indexers;
    std::map<expression, query_state> queries;
    std::map<predicate, predicate_state> predicates;
  };

  /// Spawns a partition.
  /// @param dir The directory where to store this partition on the file system.
  /// @param sink The actor receiving results of this partition.
  /// @pre `sink != invalid_actor`
  static behavior make(stateful_actor<state>* self, path dir, actor sink);
};

} // namespace vast

#endif
