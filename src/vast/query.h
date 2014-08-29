#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <unordered_map>
#include "vast/actor.h"
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/expression.h"
#include "vast/segment.h"
#include "vast/uuid.h"
#include "vast/util/trial.h"

namespace vast {

/// Receives index hits, looks up the corresponding segments in the archive,
/// and filters out results which it then sends to a sink.
class query : public actor_base
{
public:
  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query(caf::actor archive, caf::actor sink, expression ast);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  using bitstream_type = default_bitstream;

  segment const* current() const;

  caf::actor archive_;
  caf::actor sink_;
  expression ast_;
  caf::message_handler idle_;
  caf::message_handler waiting_;
  caf::message_handler extracting_;

  bitstream hits_ = bitstream{bitstream_type{}};
  bitstream processed_ = bitstream{bitstream_type{}};
  bitstream unprocessed_ = bitstream{bitstream_type{}};
  std::unique_ptr<segment::reader> reader_;
  caf::message segment_;
  std::unordered_map<type, expression> checkers_;

  double progress_ = 0.0;
  uint64_t requested_ = 0;
  bool inflight_ = false;
};

} // namespace vast

#endif
