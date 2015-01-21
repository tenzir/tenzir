#ifndef VAST_ACTOR_EXPORTER_H
#define VAST_ACTOR_EXPORTER_H

#include <set>
#include "vast/actor/actor.h"

namespace vast {

/// An actor receiving events and dispatching them to registered sinks.
class exporter : public default_actor
{
public:
  void at(caf::down_msg const& msg) override;
  caf::message_handler make_handler() override;
  std::string name() const override;

private:
  std::set<caf::actor> sinks_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace vast

#endif
