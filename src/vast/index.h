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
class index : public actor_base
{
public:
  struct dissector;
  struct builder;
  struct pusher;
  struct evaluator;

  struct evaluation
  {
    bitstream hits;
    std::map<expression, double> predicate_progress;
    double total_progress = 0.0;
  };

  struct predicate_cache_entry
  {
    struct partition_state
    {
      uint64_t got = 0;
      optional<uint64_t> expected;
      bitstream hits;
    };

    std::unordered_map<uuid, partition_state> parts;
  };

  struct partition_state
  {
    time_point first = time_range{};
    time_point last = time_range{};
  };

  struct query_state
  {
    bitstream hits;
    util::flat_set<caf::actor> subscribers;
  };

  /// Spawns the index.
  /// @param dir The VAST directory to create the index under.
  /// @param batch_size The number of events to index at once.
  index(path const& dir, size_t batch_size);

  /// Evaluates a given AST with respect to the cache.
  /// @param ast The AST to get the progress for.
  /// @returns The ::evaluation result.
  evaluation evaluate(expression const& ast);

  /// Spawns a partition.
  /// @param dir The directory of the partition.
  /// @returns `nothing` on success.
  trial<void> make_partition(path const& dir);

  /// Updates the state of a given partition.
  /// @param id The UUID of the partition to update.
  /// @param first The timestamp of the first event in partition *id*.
  /// @param last The timestamp of the last event in partition *id*.
  void update_partition(uuid const& id, time_point first, time_point last);

  /// Applies a function to node chain in the GCG up the roots.
  /// @param start The AST to begin execution.
  /// @param f The function to execute on *start* and its partents.
  /// @returns The path of nodes taken through the GCG.
  std::vector<expression> walk(
      expression const& start,
      std::function<bool(expression const&, util::flat_set<expression> const&)> f);

  caf::message_handler act() final;
  std::string describe() const final;

  path dir_;
  size_t batch_size_;
  std::map<expression, predicate_cache_entry> predicate_cache_;
  std::map<expression, util::flat_set<expression>> gqg_;
  std::map<expression, query_state> queries_;
  std::map<std::string, uuid> part_map_;
  std::unordered_map<uuid, partition_state> part_state_;
  std::unordered_map<uuid, caf::actor> part_actors_;
  std::pair<uuid, caf::actor> active_part_;
};

} // namespace vast

#endif
