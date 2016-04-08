#ifndef VAST_ACTOR_PARTITION_HPP
#define VAST_ACTOR_PARTITION_HPP

#include <map>
#include <set>
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/schema.hpp"
#include "vast/time.hpp"
#include "vast/uuid.hpp"
#include "vast/actor/accountant.hpp"
#include "vast/actor/basic_state.hpp"
#include "vast/expr/evaluator.hpp"

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
