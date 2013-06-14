#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/uuid.h"

namespace vast {

/// The ingestor. This component manages different types of event sources, each
/// of which generate events in a different manner.
class ingestor : public cppa::event_based_actor
{
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

  /// Implements `cppa::event_based_actor::init`.
  virtual void init() final;

private:
  void shutdown();

  void init_source(cppa::actor_ptr source);

  cppa::actor_ptr tracker_;
  cppa::actor_ptr archive_;
  cppa::actor_ptr index_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  size_t batch_size_;

  std::vector<cppa::actor_ptr> segmentizers_;
  std::unordered_map<cppa::actor_ptr, size_t> rates_;
  std::unordered_map<uuid, unsigned> inflight_;
  cppa::behavior operating_;
};

} // namespace vast

#endif
