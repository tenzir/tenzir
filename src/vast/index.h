#ifndef VAST_INDEX_H
#define VAST_INDEX_H

#include "vast/actor.h"
#include "vast/bitstream.h"
#include "vast/file_system.h"
#include "vast/optional.h"
#include "vast/uuid.h"
#include "vast/time.h"
#include "vast/util/flat_set.h"

namespace vast {

/// An inter-query predicate cache.
class index
{
public:
  /// A function applied to each <predicate, partition> pair that is not in the
  /// cache. Iff it returns `true`, the pair will be added to the cache.
  using miss_callback = std::function<bool(expr::ast const&, uuid const&)>;

  struct evaluation
  {
    bitstream hits;
    std::map<expr::ast, double> predicate_progress;
    double total_progress;
  };

  struct cache_entry
  {
    struct partition_state
    {
      size_t got = 0;
      optional<size_t> expected;
      bitstream hits;
    };

    std::unordered_map<uuid, partition_state> parts;
  };

  struct partition_state
  {
    time_point first;
    time_point last;
    bitstream coverage;
  };

  /// Sets the miss callback for failed partition hits lookups for a given
  /// predicate.
  /// @param f The callback.
  void set_on_miss(miss_callback f);

  /// Evaluates a given AST with respect to the cache.
  /// @param ast The AST to get the progress for.
  /// @returns The result of the evaluation.
  trial<evaluation> evaluate(expr::ast const& ast);

  /// Incorporates a new query.
  /// @param qry The query AST.
  /// @returns `true` on success.
  trial<evaluation> add_query(expr::ast const& qry);

  /// Registers the number of expected results for a given predicate/partition.
  /// @param pred The (existing) predicate to update.
  /// @param part The partition *pred* came from.
  /// @param n The number of expected hits for *pred* from *part*.
  void expect(expr::ast const& pred, uuid const& part, size_t n);

  /// Updates the state of a given partition.
  /// @param id The UUID of the partition to update.
  /// @param first The timestamp of the first event in partition *id*.
  /// @param last The timestamp of the last event in partition *id*.
  /// @param coverage The coverage of partition *id*.
  void update_partition(uuid const& id, time_point first, time_point last,
                        bitstream const& coverage);

  /// Updates the cache with new hits.
  /// @param pred The predicate to update with *hits*.
  /// @param part The partition where *pred* comes from.
  /// @param hits The new result for *pred* from *part*.
  /// @returns The queries affected by *pred*.
  std::vector<expr::ast> update_hits(expr::ast const& pred, uuid const& part,
                                     bitstream const& hits);

private:
  class dissector;
  class builder;
  class pusher;
  class evaluator;

  /// Applies a function to node chain in the GCG up the roots.
  /// @param start The AST to begin execution.
  /// @param f The function to execute on *start* and its partents.
  /// @returns The path of nodes taken through the GCG.
  std::vector<expr::ast> walk(
      expr::ast const& start,
      std::function<bool(expr::ast const&, util::flat_set<expr::ast> const&)> f);

  std::map<expr::ast, cache_entry> cache_;
  std::unordered_map<uuid, partition_state> partitions_;
  std::map<expr::ast, util::flat_set<expr::ast>> gqg_;
  util::flat_set<expr::ast> queries_;
  miss_callback on_miss_ = [](expr::ast const&, uuid const&) { return true; };
};

/// The event index.
struct index_actor : actor<index_actor>
{
  struct query_state
  {
    bitstream hits;
    util::flat_set<cppa::actor_ptr> subscribers;
  };

  /// Spawns the index.
  /// @param dir The root directory of the index.
  /// @param batch_size The number of events to index at once.
  index_actor(path dir, size_t batch_size);

  trial<nothing> make_partition(path const& dir);

  void act();
  char const* description() const;

  path dir_;
  size_t batch_size_;
  std::map<expr::ast, query_state> queries_;
  std::unordered_map<uuid, cppa::actor_ptr> part_actors_;
  std::map<string, uuid> parts_;
  std::pair<uuid, cppa::actor_ptr> active_;
  index index_;
};

} // namespace vast

#endif
