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
class query
{
public:
  /// The state of the query.
  enum query_state
  {
    idle,         ///< No unprocessed hits, no segments.
    waiting,      ///< Unprocessed hits, but no segments.
    ready,        ///< Unprocesseed hits and segments. Ready to extract.
    extracting,   ///< Extraction of events from segment in progress.
    done,         ///< Finished extraction.
    failed        ///< Failed due to an unrecoverable error.
  };

  /// Constructs a query from an ast.
  /// @param ast The query expression ast.
  /// @param fn The function to invoke for each matching event.
  query(expr::ast ast, std::function<void(event)> fn);

  /// Retrieves the query state.
  /// @returns The current state of the query.
  query_state state() const;

  /// Retrieves the number of segments the query currently buffers.
  /// @returns The number of segments the query harbors.
  size_t segments() const;

  /// Retrieves the event IDs of the next segments to retrieve.
  /// @param max The maximum number of segment to
  /// @returns The event IDs the query still needs segments for.
  std::vector<event_id> next() const;

  /// Updates the query with a new hits from the index.
  /// @param hits The new hits from the index.
  /// @post `state() == waiting` if unprocessed hits available.
  void add(bitstream hits);

  /// Adds a segment to the query.
  /// @param s The segment.
  /// @returns `true` if the segment did not exist already.
  /// @post `state() == ready` if `state() == waiting` and segment available.
  bool add(cow<segment> s);

  /// Extracts results from the current segment.
  /// @param max The maximum number of events to extract. 0 means no limit.
  /// @returns The number of processed results on success.
  /// @pre `state() == ready || state() == extracting`
  trial<uint64_t> extract(uint64_t max = 0);

  /// Tells the query that all hits have arrived.
  /// @post `state() == done` if state was `idle` and no more unprocessed hits.
  void finish();

private:
  using bitstream_type = default_bitstream;

  bool instantiate();

  bool finishing_ = false;
  query_state state_ = idle;
  expr::ast ast_;
  std::function<void(event)> fn_;
  bitstream hits_;
  bitstream processed_;
  bitstream unprocessed_;
  bitstream masked_;
  cow<segment> current_;
  std::unique_ptr<segment::reader> reader_;
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

  void prefetch(size_t max);
  void extract(uint64_t n);

  cppa::actor_ptr archive_;
  cppa::actor_ptr sink_;
  query query_;

  std::vector<event_id> inflight_;
  uint64_t requested_ = 10; // TODO: make initial batch configurable.
  size_t prefetch_ = 2; // TODO: make configurable.
};

} // namespace vast

#endif
