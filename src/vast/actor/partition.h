#ifndef VAST_ACTOR_PARTITION_H
#define VAST_ACTOR_PARTITION_H

#include <map>
#include <set>
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/filesystem.h"
#include "vast/schema.h"
#include "vast/time.h"
#include "vast/trial.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"
#include "vast/expr/evaluator.h"

namespace vast {

/// A horizontal partition of the index.
///
/// For each chunk PARTITION receives, it spawns (i) a dedicated DECHUNKIFIER
/// which turns the chunk back into a sequence of events, (ii) a set of
/// EVENT_INDEXERs for all event types occurring in the chunk, and (iii)
/// registers all EVENT_INDEXERs as sink of the DECHUNKIFIER.
struct partition : public flow_controlled_actor
{
  using bitstream_type = default_bitstream;

  struct evaluator : expr::bitstream_evaluator<evaluator, default_bitstream>
  {
    evaluator(partition const& p);
    bitstream_type const* lookup(predicate const& pred) const;

    partition const& partition_;
  };

  /// Spawns a partition.
  /// @param dir The directory where to store this partition on the file system.
  /// @param sink The actor receiving results of this partition.
  /// @pre `sink != invalid_actor`
  partition(path dir, caf::actor sink);

  void at(caf::down_msg const& msg) override;
  void at(caf::exit_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  void flush(caf::actor const& task);

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

  path const dir_;
  caf::actor sink_;
  caf::actor proxy_;
  schema schema_;
  size_t chunks_indexed_concurrently_ = 0;
  std::multimap<event_id, caf::actor> indexers_;
  std::map<expression, query_state> queries_;
  std::map<predicate, predicate_state> predicates_;
  std::set<caf::actor_addr> dechunkifiers_;
};

} // namespace vast

#endif
