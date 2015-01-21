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
class tracker : public default_actor
{
public:
  /// Spawns a tracker.
  /// @param dir The directory to use for meta data.
  tracker(path dir);

  void at(caf::down_msg const& msg) override;
  void at(caf::exit_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  enum class component
  {
    invalid,
    importer,
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

  path dir_;
  caf::actor identifier_;
  std::map<std::string, actor_state> actors_;
  std::multimap<std::string, std::string> topology_;
};

} // namespace vast

#endif
