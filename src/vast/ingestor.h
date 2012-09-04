#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include <ze/uuid.h>

namespace vast {

/// The ingestor. This component manages different types of event sources, each
/// of which generate events in a different manner.
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
  void shutdown();

  size_t max_events_per_chunk_ = 0;
  size_t max_segment_size_ = 0;
  size_t batch_size_ = 0;

  std::vector<cppa::actor_ptr> sources_;
  std::unordered_map<cppa::actor_ptr, size_t> rates_;
  std::unordered_map<ze::uuid, unsigned> inflight_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::actor_ptr broccoli_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
