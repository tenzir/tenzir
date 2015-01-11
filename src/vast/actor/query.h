#ifndef VAST_ACTOR_QUERY_H
#define VAST_ACTOR_QUERY_H

#include <unordered_map>
#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"
#include "vast/util/trial.h"

namespace vast {

/// Receives index hits, looks up the corresponding chunks in the archive, and
/// filters out results which it then sends to a sink.
class query : public actor_mixin<query>
{
public:
  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query(caf::actor archive, caf::actor sink, expression ast);

  caf::message_handler make_handler();
  std::string name() const;

private:
  using bitstream_type = default_bitstream;

  caf::actor archive_;
  caf::actor sink_;
  expression ast_;
  caf::message_handler idle_;
  caf::message_handler waiting_;
  caf::message_handler extracting_;

  bitstream hits_ = bitstream{bitstream_type{}};
  bitstream processed_ = bitstream{bitstream_type{}};
  bitstream unprocessed_ = bitstream{bitstream_type{}};
  std::unordered_map<type, expression> expressions_;
  std::unique_ptr<chunk::reader> reader_;
  chunk chunk_;

  double progress_ = 0.0;
  uint64_t requested_ = 0;
  bool inflight_ = false;
};

} // namespace vast

#endif
