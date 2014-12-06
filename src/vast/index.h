#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/expression.h"
#include "vast/file_system.h"
#include "vast/optional.h"
#include "vast/uuid.h"
#include "vast/time.h"
#include "vast/util/flat_set.h"

namespace vast {

/// An inter-query predicate cache.
class index : public actor_mixin<index, flow_controlled, sentinel>
{
public:
  struct predicatizer;
  struct builder;
  struct pusher;
  struct dispatcher;
  struct evaluator;
  struct propagator;

  struct partition_state
  {
    struct predicate_status
    {
      bitstream hits;
      uint64_t got = 0;
      optional<uint64_t> expected;
    };

    std::map<expression, predicate_status> status;
    caf::actor actor;
    uint64_t events = 0;
    time_point last_modified;
    time_point first_event = time_range{};
    time_point last_event = time_range{};

  private:
    friend access;
    void serialize(serializer& sink) const;
    void deserialize(deserializer& source);
  };

  struct query_state
  {
    struct predicate_state
    {
      bitstream hits;
      std::vector<uuid> restrictions;
    };

    std::map<expression, predicate_state> predicates;
    util::flat_set<caf::actor> subscribers;
  };

  struct schedule_state
  {
    uuid part;
    util::flat_set<predicate> predicates;
  };

  /// Spawns the index.
  /// @param dir The directory of the index.
  /// @param batch_size The number of events to index at once per partition.
  /// @param max_events The maximum number of events per partition.
  /// @param max_parts The maximum number of partitions to hold in memory.
  /// @param active_parts The number of active partitions to hold in memory.
  index(path const& dir, size_t batch_size, size_t max_events,
        size_t max_parts, size_t active_parts);

  /// Dispatches a predicate for a partition either by relaying it directory
  /// if active or enqueing it into partition queue.
  /// @param part The partition to query with *pred*.
  /// @param pred The predicate to look for in *part*.
  void dispatch(uuid const& part, predicate const& pred);

  /// Consolidates a predicate which has previously been dispatched.
  /// @param part The partition of *pred*.
  /// @param pred The predicate which delivered all hits within *part*.
  /// @pre The combination of *part* and *pred* must have been dispatched.
  void consolidate(uuid const& part, predicate const& pred);

  /// Computes the progression for a given query.
  double progress(expression const& query) const;

  // FIXME: only propagate overload upstream if *all* partitions are
  // overloaded. This requires tracking the set of overloaded partitions
  // manually.

  void at_down(caf::down_msg const& msg);
  void at_exit(caf::exit_msg const& msg);
  caf::message_handler make_handler();
  std::string name() const;

  path dir_;
  size_t batch_size_;
  size_t max_events_per_partition_;
  size_t max_partitions_;
  size_t active_partitions_;
  std::multimap<expression, std::shared_ptr<expression>> predicates_;
  std::map<expression, query_state> queries_;
  std::unordered_map<uuid, partition_state> partitions_;
  std::list<schedule_state> schedule_;
  std::list<uuid> passive_;
  std::vector<uuid> active_;
  size_t next_ = 0;
};

} // namespace vast

#endif
