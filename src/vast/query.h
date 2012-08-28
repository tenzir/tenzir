#ifndef VAST_QUERY_H
#define VAST_QUERY_H

#include <string>
#include <cppa/cppa.hpp>
#include "vast/expression.h"
#include "vast/segment.h"

namespace vast {

/// The query.
class query : public cppa::sb_actor<query>
{
  friend class cppa::sb_actor<query>;

public:
  struct statistics
  {
    uint64_t evaluated = 0;
    uint64_t results = 0;
    uint64_t batch = 0;
  };

  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param sink The sink receiving the query results.
  /// @param expr The query expression.
  query(cppa::actor_ptr archive,
        cppa::actor_ptr index,
        cppa::actor_ptr sink,
        expression expr);

private:
  /// A window of segments for extracting events.
  class window
  {
  public:
    /// Constructs an empty window.
    window() = default;

    /// Determines whether the window is ready to extract events.
    /// @return `true` *iff* at 1 or more events can be extracted.
    bool ready() const;

    /// Adds a new segment to the window.
    void add(cppa::cow_tuple<segment> s);

    /// Tries to extract an event from the current segment.
    ///
    /// @param event Receives the next event.
    ///
    /// @return `true` if extracting an event from the current segment into
    /// *event* succeeded and `false` if there are no more events in the
    /// current segment.
    bool extract(ze::event& event);

  private:
    std::deque<cppa::cow_tuple<segment>> segments_;
    std::unique_ptr<segment::reader> reader_;
  };

  bool running_ = true;
  uint64_t batch_size_ = 100; // TODO: adapt based on processing time.
  expression expr_;
  statistics stats_;

  std::vector<ze::uuid> ids_;
  size_t head_ = 0;
  size_t ack_ = 0;
  size_t window_size_ = 5; // TODO: make configurable.
  window window_;

  cppa::actor_ptr archive_;
  cppa::actor_ptr sink_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
