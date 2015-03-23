#ifndef VAST_ACTOR_TRACKER_H
#define VAST_ACTOR_TRACKER_H

#include <map>
#include <string>
#include <vector>
#include "vast/filesystem.h"
#include "vast/actor/actor.h"
#include "vast/util/flat_set.h"

namespace vast {

/// Manages topology within a VAST ecosystem.
struct tracker : default_actor
{
  enum class component
  {
    invalid,
    source,
    exporter,
    receiver,
    archive,
    index,
    search
  };

  struct actor_state
  {
    caf::actor actor;
    component type;
  };

  /// Spawns a tracker.
  /// @param dir The directory to use for meta data.
  tracker(path dir);

  void on_exit();
  caf::behavior make_behavior() override;

  path dir_;
  caf::actor identifier_;
  std::map<std::string, actor_state> actors_;
  std::multimap<std::string, std::string> topology_;
};

} // namespace vast

#endif
