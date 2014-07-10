#ifndef VAST_IMPORTER_H
#define VAST_IMPORTER_H

#include <queue>
#include <unordered_map>
#include <cppa/cppa.hpp>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/uuid.h"
#include "vast/segmentizer.h"

namespace vast {

/// Manages sources which produce events.
class importer : public actor_base
{
public:
  /// Spawns an importer.
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
  importer(path dir,
           cppa::actor receiver,
           size_t max_events_per_chunk,
           size_t max_segment_size,
           uint64_t batch_size);

  cppa::partial_function act() final;
  std::string describe() const final;

private:
  path dir_;
  cppa::actor receiver_;
  cppa::actor source_;
  cppa::actor segmentizer_;
  cppa::partial_function ready_;
  cppa::partial_function paused_;
  cppa::partial_function terminating_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  uint64_t batch_size_;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
