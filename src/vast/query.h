#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/cow.h"
#include "vast/expression.h"
#include "vast/util/range_map.h"

namespace vast {

class segment;

/// Takes bitstreams and segments to produce results in the form of events.
class query
{
public:
  /// Constructs a query from an ast.
  /// @param ast The query expression ast.
  query(expr::ast ast);

  /// Updates the query with a new result from the index.
  /// @param result The new result from the index.
  /// @returns The first event ID of the computed delta result.
  void update(bitstream result);

  /// Adds a segment to the query.
  /// @param s The segment.
  /// @returns `true` if the segment did not exist already.
  bool add(cow<segment> s);

  /// Checks whether the query has a segment for the current event ID.
  /// @returns `true` if the query is ready to process results.
  bool executable() const;

  /// Applies a function over each extracted event from a given segment.
  /// @param f The function to apply to each event.
  /// @returns The number of events that *f* was applied to.
  size_t process(std::function<void(event)> f);

  /// Retrieves the current event ID.
  /// @returns The current position of the query in the event ID space.
  event_id current() const;

private:
  bitstream hits_;
  bitstream processed_;
  bitstream unprocessed_;
  event_id current_ = 0;
  expr::ast ast_;
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
  event_id current_;
};

} // namespace vast

#endif
