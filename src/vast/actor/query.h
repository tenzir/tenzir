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
struct query : public default_actor
{
  using bitstream_type = default_bitstream;

  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param sink The sink receiving the query results.
  /// @param ast The query expression ast.
  query(caf::actor archive, caf::actor sink, expression ast);

  caf::message_handler make_handler() override;
  std::string name() const override;

  // Prefetches the next chunk and sets the "inflight" chunk status. If we
  // don't have a chunk yet, we look for the chunk corresponding to the last
  // unprocessed hit. If we have a chunk, we try to get the next chunk in the
  // ID space. If no such chunk exists, we try to get a chunk located before
  // the current one. If neither exist, we don't do anything.
  void prefetch();

  caf::actor archive_;
  caf::actor sink_;
  caf::actor task_;
  expression ast_;
  caf::message_handler idle_;
  caf::message_handler waiting_;
  caf::message_handler extracting_;

  bitstream_type hits_;
  bitstream_type processed_;
  bitstream_type unprocessed_;
  std::unordered_map<type, expression> expressions_;
  std::unique_ptr<chunk::reader> reader_;
  chunk chunk_;

  time::point start_time_;
  double progress_ = 0.0;
  uint64_t requested_ = 0;
  bool inflight_ = false;
};

} // namespace vast

#endif
