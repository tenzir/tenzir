#ifndef VAST_ACTOR_EXPORTER_H
#define VAST_ACTOR_EXPORTER_H

#include <memory>
#include <unordered_map>

#include "vast/aliases.h"
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/expression.h"
#include "vast/query_options.h"
#include "vast/uuid.h"
#include "vast/actor/accountant.h"
#include "vast/actor/basic_state.h"
#include "vast/util/flat_set.h"

namespace vast {

/// Receives index hits, looks up the corresponding chunks in the archive, and
/// filters out results which it then sends to a sink.
struct exporter {
  using bitstream_type = decltype(chunk::meta_data::ids);

  struct state : basic_state {
    state(local_actor* self);

    util::flat_set<actor> archives;
    util::flat_set<actor> indexes;
    util::flat_set<actor> sinks;
    accountant::type accountant;
    bool draining = false;
    bool inflight = false;
    double progress = 0.0;
    uint64_t requested = 0;
    uint64_t total_hits = 0;
    uint64_t total_results = 0;
    uint64_t total_chunks = 0;
    uint64_t chunk_results = 0;
    bitstream_type hits;
    bitstream_type unprocessed;
    std::unordered_map<type, expression> checkers;
    std::unique_ptr<chunk::reader> reader;
    chunk current_chunk;
    uuid const id;
    time::moment start_time;
  };

  /// Spawns an EXPORTER.
  /// @param self The actor handle.
  /// @param ast The AST of query.
  /// @param qos The query options.
  static behavior make(stateful_actor<state>* self, expression expr,
                       query_options opts);
};

} // namespace vast

#endif
