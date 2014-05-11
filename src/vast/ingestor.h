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
  char const* describe() const final;

private:
  enum state
  {
    ready,
    paused,
    waiting
  };

  path dir_;
  state state_ = ready;
  cppa::actor receiver_;
  cppa::actor source_;
  cppa::actor sink_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  uint64_t batch_size_;
  bool terminating_ = false;
  bool backlogged_ = false;
  std::queue<cppa::any_tuple> buffer_;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
