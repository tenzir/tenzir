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
    uint64_t processed = 0;
    uint64_t matched = 0;
  };

  /// Spawns a query actor.
  /// @param archive The archive actor.
  /// @param index The index actor.
  /// @param index The sink receiving the results.
  query(cppa::actor_ptr archive, cppa::actor_ptr index, cppa::actor_ptr sink);

private:
  /// A window of segments for extracting events.
  class window
  {
  public:
    /// Constructs an empty window.
    window() = default;

    /// Adds a new segment to the window.
    void push(cppa::cow_tuple<segment> s);

    /// Determines whether the window can extract events from the current
    /// segment.
    /// @return `true` if the the
    bool ready() const;

    /// Determines wheter the window is *stale*, i.e., is not ready and has no
    /// more segments to advance to.
    bool stale() const;

    /// Tries to extract an event from the current segment.
    ///
    /// @param event Receives the next event.
    ///
    /// @return `true` if extracting an event from the current segment into
    /// *event* succeeded and `false` if there are no more events in the
    /// current segment.
    bool one(ze::event& event);

    /// Returns the current window size;
    /// @return The number of available segments in the window.
    size_t size() const;

    /// Tries to advance to the next segment.
    ///
    /// @return `true` *iff* there exists at least one more segment to advance
    /// to.
    ///
    /// @post If the function returns `true`, the next call to window::one()
    /// also returns `true` because empty chunks/segments do not exist.
    bool advance();

  private:
    std::deque<cppa::cow_tuple<segment>> segments_;
    segment const* current_segment_ = nullptr;
    std::unique_ptr<segment::reader> reader_;
  };

  /// Parses the query expression and replies with success or failure.
  /// @param The query expression.
  void parse(std::string const& expr);

  /// Ask the index, receive segment IDs, and query the archive with them.
  void run();

  /// Tries to extract a given number of events from the window.
  void extract(size_t n);

  /// Tries to match an event and send it to the sink.
  bool match(ze::event const& event);

  size_t batch_size_ = 0;
  expression expr_;
  statistics stats_;
  window window_;
  size_t window_size_ = 5; // FIXME: make configurable.

  std::vector<ze::uuid> ids_;
  std::vector<ze::uuid>::const_iterator head_;
  std::vector<ze::uuid>::const_iterator ack_;

  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr sink_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
