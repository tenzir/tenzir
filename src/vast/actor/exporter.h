#ifndef VAST_ACTOR_EXPORTER_H
#define VAST_ACTOR_EXPORTER_H

#include <unordered_map>

#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/uuid.h"
#include "vast/actor/accountant.h"
#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

/// Receives index hits, looks up the corresponding chunks in the archive, and
/// filters out results which it then sends to a sink.
struct exporter : default_actor {
  using bitstream_type = decltype(chunk::meta_data::ids);

  /// Spawns an EXPORTER.
  /// @param ast The AST of query.
  /// @param opts The query options.
  exporter(expression ast, query_options opts);

  void on_exit() override;
  behavior make_behavior() override;

  // Prefetches the next chunk and sets the "inflight" chunk status. If we
  // don't have a chunk yet, we look for the chunk corresponding to the last
  // unprocessed hit. If we have a chunk, we try to get the next chunk in the
  // ID space. If no such chunk exists, we try to get a chunk located before
  // the current one. If neither exist, we don't do anything.
  void prefetch();

  util::flat_set<actor> archives_;
  util::flat_set<actor> indexes_;
  util::flat_set<actor> sinks_;
  accountant::actor_type accountant_;
  message_handler init_;
  message_handler idle_;
  message_handler waiting_;
  message_handler extracting_;

  bool draining_ = false;
  bool inflight_ = false;
  double progress_ = 0.0;
  uint64_t pending_ = 0;
  uint64_t total_hits_ = 0;
  uint64_t total_results_ = 0;
  bitstream_type hits_;
  bitstream_type processed_;
  bitstream_type unprocessed_;
  std::unordered_map<type, expression> expressions_;
  std::unique_ptr<chunk::reader> reader_;
  chunk chunk_;

  uuid const id_;
  expression ast_;
  query_options opts_;
  time::moment start_time_;
};

} // namespace vast

#endif
