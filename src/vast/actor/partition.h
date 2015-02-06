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

namespace vast {

/// A horizontal partition of the index.
///
/// For each chunk PARTITION receives, it spawns (i) a dedicated DECHUNKIFIER
/// which turns the chunk back into a sequence of events, (ii) a set of
/// EVENT_INDEXERs for all event types occurring in the chunk, and (iii)
/// registers all EVENT_INDEXERs as sink of the DECHUNKIFIER.
class partition : public flow_controlled_actor
{
  struct evaluator;

public:
  using bitstream_type = default_bitstream;

  /// Spawns a partition.
  /// @param dir The directory where to store this partition on the file system.
  partition(path const& dir);

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
    util::flat_set<expression> queries;
  };

  struct query_state
  {
    caf::actor task;
    bitstream_type hits;
    util::flat_set<caf::actor> sinks;
    size_t chunks = 0;
  };

  path const dir_;
  schema schema_;
  std::multimap<caf::actor_addr, caf::actor> dechunkifiers_;
  std::multimap<event_id, caf::actor> indexers_;
  std::map<predicate, predicate_state> predicates_;
  std::map<expression, query_state> queries_;
  std::map<caf::actor_addr, predicate const*> predicate_tasks_;
  std::map<caf::actor_addr, expression const*> query_tasks_;
  std::set<caf::actor_addr> inflight_pings_;
};

} // namespace vast

#endif
