#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/cow.h"
#include "vast/expression.h"

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

  /// Advances the current position in the ID space by a given value.
  /// @param n The number of events to fast-forward.
  /// @returns The next
  event_id advance(size_t n);

  /// Applies a function over each extracted event from a given segment.
  /// @param s The segment to extract events from.
  /// @param f The function to apply to each event.
  /// @returns The number of events that *f* was applied to.
  size_t apply(cow<segment> const& s, std::function<void(event)> f);

  /// Retrieves the current event ID.
  /// @returns The current position of the query in the event ID space.
  event_id current() const;

private:
  bitstream result_;
  bitstream processed_;
  event_id current_ = 0;
  expr::ast ast_;
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
