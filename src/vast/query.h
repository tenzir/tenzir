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
class query : public actor_base
{
public:
  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query(cppa::actor archive, cppa::actor sink, expr::ast ast);

  cppa::behavior act() final;
  std::string describe() const final;

private:
  using bitstream_type = default_bitstream;

  segment const* current() const;

  cppa::actor archive_;
  cppa::actor sink_;
  expr::ast ast_;
  cppa::behavior idle_;
  cppa::behavior waiting_;
  cppa::behavior extracting_;

  bitstream hits_ = bitstream{bitstream_type{}};
  bitstream processed_ = bitstream{bitstream_type{}};
  bitstream unprocessed_ = bitstream{bitstream_type{}};
  std::unique_ptr<segment::reader> reader_;
  cppa::any_tuple segment_;

  uint64_t requested_ = 0;
  bool inflight_ = false;
};

} // namespace vast

#endif
