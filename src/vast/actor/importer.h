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
class importer : public default_actor
{
public:
  /// Spawns an importer.
  /// @param dir The directory where to save persistent state.
  /// @param batch_size The number of events a synchronous source buffers until
  ///                   relaying them to the chunkifier
  /// @param method The compression method to use for the chunkifier.
  importer(path dir, uint64_t batch_size, io::compression method);

  void at(caf::exit_msg const& msg) override;
  void at(caf::down_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  path dir_;
  io::compression compression_;
  size_t current_ = 0;
  std::vector<caf::actor> sinks_;
  caf::actor source_;
  caf::actor chunkifier_;
  caf::message_handler ready_;
  caf::message_handler paused_;
  caf::message_handler terminating_;
  uint64_t batch_size_;
  size_t stored_ = 0;
  std::set<path> orphaned_;
};

} // namespace vast

#endif
