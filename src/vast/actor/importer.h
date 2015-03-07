#ifndef VAST_ACTOR_IMPORTER_H
#define VAST_ACTOR_IMPORTER_H

#include <queue>
#include <unordered_map>
#include <caf/all.hpp>
#include "vast/filesystem.h"
#include "vast/uuid.h"
#include "vast/actor/actor.h"

namespace vast {

/// Manages sources which produce events.
struct importer : flow_controlled_actor
{
  /// Spawns an importer.
  /// @param dir The directory where to save persistent state.
  /// @param chunk_size The number of events a source buffers until
  ///                   relaying them to the chunkifier
  /// @param method The compression method to use for the chunkifier.
  importer(path dir, uint64_t chunk_size, io::compression method);

  void on_exit();
  caf::behavior make_behavior() override;

  path dir_;
  uint64_t chunk_size_;
  io::compression compression_;
  caf::actor sink_pool_;
  caf::actor source_;
  caf::actor chunkifier_;
  caf::actor accountant_;
  caf::message_handler terminating_;
  size_t stored_ = 0;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
