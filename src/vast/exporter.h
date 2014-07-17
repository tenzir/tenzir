#ifndef VAST_EXPORTER_H
#define VAST_EXPORTER_H

#include <set>
#include "vast/actor.h"

namespace vast {

/// An actor receiving events and dispatching them to registered sinks.
class exporter : public actor_base
{
public:
  caf::message_handler act() final;
  std::string describe() const final;

private:
  std::set<caf::actor> sinks_;
  uint64_t processed_ = 0;
  uint64_t limit_ = 0;
};

} // namespace vast

#endif
