#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/cow.h"
#include "vast/expression.h"
#include "vast/segment.h"
#include "vast/uuid.h"
#include "vast/util/range_map.h"

namespace vast {

/// Takes bitstreams and segments to produce results in the form of events.
class query
{
public:
  /// Constructs a query from an ast.
  /// @param ast The query expression ast.
  /// @param fn The function to invoke for each matching event.
  query(expr::ast ast, std::function<void(event)> fn);

  /// Updates the query with a new result from the index.
  /// @param result The new result from the index.
  /// @returns The first event ID of the computed delta result.
  void update(bitstream result);

  /// Adds a segment to the query.
  /// @param s The segment.
  /// @returns `true` if the segment did not exist already.
  bool add(cow<segment> s);

  /// Purges segments according to the given boundaries.
  /// @param before The number of segments to purge before the cursor.
  /// @param after The number of segments to purge after the cursor.
  /// @returns The number of segments purged.
  size_t consolidate(size_t before, size_t after);

  /// Extracts a given number of results.
  /// @param n The maximum number of results to process before returning.
  /// @returns The number of processed results (less than or equal to *n*).
  size_t extract(size_t n = 0);

  /// Scans for uncovered hits at the borders of all known segments.
  /// @returns A list of event IDs for which the query still needs segments.
  std::vector<event_id> scan() const;

  /// Retrieves the event ID of the next unprocessed hit.
  /// @returns The current position of the query in the event ID space.
  event_id cursor() const;

  /// Retrieves the number segments the query buffers.
  /// @returns The number of cached segments in the query.
  size_t segments() const;

private:
  expr::ast ast_;
  std::function<void(event)> fn_;
  bitstream hits_;
  bitstream processed_;
  bitstream unprocessed_;
  event_id cursor_ = bitstream::npos;
  std::deque<cow<segment>> segments_;
  cow<segment> current_;
  std::unique_ptr<segment::reader> reader_;
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
  uint64_t batch_size_ = 0;
};

} // namespace vast

#endif
