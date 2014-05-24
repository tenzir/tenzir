#ifndef VAST_INGESTOR_H
#define VAST_INGESTOR_H

#include <queue>
#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/segmentizer.h"

namespace vast {

/// The ingestor. This component manages different types of event sources, each
/// of which generate events in a different manner.
class ingestor_actor : public actor_base
{
public:
  /// Spawns an ingestor.
  ///
  /// @param dir The directory where to save persistent state.
  ///
  /// @param receiver The actor receiving the generated segments.
  ///
  /// @param max_events_per_chunk The maximum number of events per chunk.
  ///
  /// @param max_segment_size The maximum size of a segment.
  ///
  /// @param batch_size The number of events a synchronous source buffers until
  /// relaying them to the segmentizer
  ingestor_actor(path dir,
                 cppa::actor receiver,
                 size_t max_events_per_chunk,
                 size_t max_segment_size,
                 uint64_t batch_size);

  cppa::behavior act() final;
  std::string describe() const final;

private:
  path dir_;
  cppa::actor receiver_;
  cppa::actor source_;
  cppa::actor segmentizer_;
  cppa::behavior ready_;
  cppa::behavior paused_;
  cppa::behavior terminating_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  uint64_t batch_size_;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
