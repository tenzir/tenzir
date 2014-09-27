#ifndef VAST_TRACKER_H
#define VAST_TRACKER_H

#include <map>
#include <string>
#include <vector>
#include "vast/actor.h"
#include "vast/file_system.h"

namespace vast {

/// Manages topology within a VAST ecosystem.
class tracker : public actor_base
{
public:
  /// Spawns a tracker.
  /// @param dir The directory to use for meta data.
  tracker(path dir);

  caf::message_handler act() final;
  std::string describe() const final;

private:
  path dir_;
  caf::actor identifier_;
  std::map<std::string, std::vector<caf::actor>> ingestion_receivers_;
  std::map<std::string, std::vector<caf::actor>> ingestion_archives_;
  std::map<std::string, std::vector<caf::actor>> ingestion_indexes_;
  std::map<std::string, std::vector<caf::actor>> retrieval_receivers_;
  std::map<std::string, std::vector<caf::actor>> retrieval_archives_;
  std::map<std::string, std::vector<caf::actor>> retrieval_indexes_;
};

} // namespace vast

#endif
