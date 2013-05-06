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
  /// Spawns an ingestor.
  /// @param tracker The ID tracker.
  /// @param archive The archive actor.
  /// @param index The index actor.
  ingestor(cppa::actor_ptr tracker,
           cppa::actor_ptr archive,
           cppa::actor_ptr index,
           size_t max_events_per_chunk,
           size_t max_segment_size,
           size_t batch_size);

private:
  void shutdown();

  std::vector<cppa::actor_ptr> segmentizers_;
  std::unordered_map<cppa::actor_ptr, size_t> rates_;
  std::unordered_map<ze::uuid, unsigned> inflight_;
  cppa::actor_ptr tracker_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  cppa::behavior init_state;
};

} // namespace vast

#endif
