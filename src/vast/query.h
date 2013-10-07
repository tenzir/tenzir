#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/expression.h"
#include "vast/segment.h"
#include "vast/uuid.h"

namespace vast {

/// The query.
class query : public actor<query>
{
public:
  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query(cppa::actor_ptr archive,
        cppa::actor_ptr index,
        cppa::actor_ptr sink,
        expr::ast ast);

  void act();
  char const* description() const;

private:
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr sink_;
  expr::ast ast_;
};

} // namespace vast

#endif
