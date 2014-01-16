#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/cow.h"
#include "vast/expression.h"
#include "vast/segment.h"
#include "vast/uuid.h"
#include "vast/util/range_map.h"
#include "vast/util/trial.h"

namespace vast {

/// Takes bitstreams and segments to produce results in the form of events.
class query
{
public:
  /// Constructs a query from an ast.
  /// @param ast The query expression ast.
  /// @param fn The function to invoke for each matching event.
  query(expr::ast ast, std::function<void(event)> fn);

  /// Updates the query with a new hits from the index.
  /// @param hits The new hits from the index.
  void update(bitstream hits);

  /// Adds a segment to the query.
  /// @param s The segment.
  /// @returns `true` if the segment did not exist already.
  bool add(cow<segment> s);

  /// Purges segments according to the given boundaries.
  /// @param before The number of segments to keep before the current one.
  /// @param after The number of segments to keep after the current one.
  void consolidate(size_t before = 1, size_t after = 1);

  /// Extracts results from the current segment.
  /// @returns The number of processed results on success.
  trial<size_t> extract();

  /// Scans for event IDs of unprocessed hits. If the query has no buffered
  /// segments, then the result only includes the singleton set with the last
  /// unprocessed hit. Otherwise the function searches for hits at the borders
  /// of known segments.
  ///
  /// @returns A list of event IDs for which the query still needs segments.
  std::vector<event_id> scan() const;

private:
  using bitstream_type = default_encoded_bitstream;

  expr::ast ast_;
  std::function<void(event)> fn_;
  bitstream hits_;
  bitstream processed_;
  bitstream unprocessed_;
  bitstream masked_;
  cow<segment> current_;
  std::unique_ptr<segment::reader> reader_;
  util::range_map<event_id, cow<segment>> segments_;
};

struct query_actor : actor<query_actor>
{
  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query_actor(cppa::actor_ptr archive, cppa::actor_ptr sink, expr::ast ast);

  void act();
  char const* description() const;

  cppa::actor_ptr archive_;
  cppa::actor_ptr sink_;
  query query_;
};

} // namespace vast

#endif
