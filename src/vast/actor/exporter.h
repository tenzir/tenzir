#ifndef VAST_ACTOR_EXPORTER_H
#define VAST_ACTOR_EXPORTER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// An actor receiving events and dispatching them to registered sinks.
class exporter : public actor_mixin<exporter, sentinel>
{
public:
  void at_down(caf::down_msg const& msg);
  caf::message_handler make_handler();
  std::string name() const;

private:
  std::set<caf::actor> sinks_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace vast

#endif
