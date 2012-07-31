#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <cppa/cppa.hpp>

namespace vast {

/// The ingestion component.
class ingestor : public cppa::sb_actor<ingestor>
{
  friend class cppa::sb_actor<ingestor>;

public:
  /// Spawns the ingestor.
  /// @param tracker The ID tracker.
  /// @param archive The archive actor.
  /// @param archive The index actor.
  ingestor(cppa::actor_ptr tracker,
           cppa::actor_ptr archive,
           cppa::actor_ptr index);

private:
  void forward(cppa::any_tuple s);
  void remove(cppa::actor_ptr src);
  void shutdown();

  size_t max_events_per_chunk_ = 0; ///< The maximum number of events per chunk.
  size_t max_segment_size_ = 0;     ///< The maximum segment size in bytes.
  bool terminating_ = false;

  std::vector<cppa::actor_ptr> sources_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr broccoli_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
