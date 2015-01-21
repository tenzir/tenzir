#ifndef VAST_ACTOR_IDENTIFIER_H
#define VAST_ACTOR_IDENTIFIER_H

#include "vast/actor/actor.h"
#include "vast/aliases.h"
#include "vast/filesystem.h"

namespace vast {

/// Keeps track of the event ID space.
class identifier : public default_actor
{
public:
  /// Constructs the ID tracker.
  /// @param dir The directory where to save the ID to.
  identifier(path dir);

  void at(caf::exit_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  bool save();

  path dir_;
  event_id id_ = 0;
};

} // namespace vast

#endif
