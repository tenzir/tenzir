#ifndef VAST_IMPORTER_H
#define VAST_IMPORTER_H

#include <queue>
#include <unordered_map>
#include <caf/all.hpp>
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
           caf::actor receiver,
           size_t max_events_per_chunk,
           size_t max_segment_size,
           uint64_t batch_size);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  path dir_;
  caf::actor receiver_;
  caf::actor source_;
  caf::actor segmentizer_;
  caf::message_handler ready_;
  caf::message_handler paused_;
  caf::message_handler terminating_;
  size_t max_events_per_chunk_;
  size_t max_segment_size_;
  uint64_t batch_size_;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
