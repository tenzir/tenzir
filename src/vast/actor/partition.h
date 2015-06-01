#ifndef VAST_ACTOR_PARTITION_H
#define VAST_ACTOR_PARTITION_H

#include <map>
#include <set>
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"
#include "vast/expr/evaluator.h"

namespace vast {

/// A horizontal partition of the index.
///
/// For each event batch PARTITION receives, it spawns one EVENT_INDEXERs per
/// type occurring in the batch and forwards them the events. 
struct partition : flow_controlled_actor
{
  using bitstream_type = default_bitstream;

  struct evaluator : expr::bitstream_evaluator<evaluator, default_bitstream>
  {
    evaluator(partition const& p);
    bitstream_type const* lookup(predicate const& pred) const;

    partition const& partition_;
  };

  struct predicate_state
  {
    caf::actor task;
    bitstream_type hits;
    util::flat_set<event_id> coverage;
    util::flat_set<expression const*> queries;
  };

  struct query_state
  {
    caf::actor task;
    bitstream_type hits;
  };

  /// Spawns a partition.
  /// @param dir The directory where to store this partition on the file system.
  /// @param sink The actor receiving results of this partition.
  /// @pre `sink != invalid_actor`
  partition(path dir, caf::actor sink);

  void on_exit();
  caf::behavior make_behavior() override;

  void flush();

  path const dir_;
  caf::actor sink_;
  caf::actor proxy_;
  schema schema_;
  size_t events_indexed_concurrently_ = 0;
  std::multimap<event_id, caf::actor> indexers_;
  std::map<expression, query_state> queries_;
  std::map<predicate, predicate_state> predicates_;
};

} // namespace vast

#endif
