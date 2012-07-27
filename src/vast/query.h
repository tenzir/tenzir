#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include <cppa/cppa.hpp>
#include "vast/expression.h"

namespace vast {

/// The query.
class query : public cppa::sb_actor<query>
{
  friend class cppa::sb_actor<query>;

public:
  struct statistics
  {
    uint64_t processed = 0;
    uint64_t matched = 0;
  };

  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param index The sink receiving the results.
  /// @param str The query expression.
  query(cppa::actor_ptr archive,
        cppa::actor_ptr index,
        cppa::actor_ptr sink,
        std::string str);

private:
  std::string str_;
  expression expr_;
  uint64_t batch_size_;
  statistics stats_;

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr sink_;
  cppa::actor_ptr source_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
