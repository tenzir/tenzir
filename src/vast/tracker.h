#ifndef VAST_TRACKER_H
#define VAST_TRACKER_H

#include <map>
#include <string>
#include <vector>
#include "vast/actor.h"
#include "vast/file_system.h"
#include "vast/util/flat_set.h"

namespace vast {

/// Manages topology within a VAST ecosystem.
class tracker : public actor_mixin<tracker, sentinel>
{
public:
  /// Spawns a tracker.
  /// @param dir The directory to use for meta data.
  tracker(path dir);

  caf::message_handler make_handler();
  void at_down(caf::down_msg const& msg);
  std::string name() const;

private:
  enum class component
  {
    importer,
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

  path dir_;
  caf::actor identifier_;
  std::map<std::string, actor_state> actors_;
  std::multimap<std::string, std::string> topology_;
};

} // namespace vast

#endif
