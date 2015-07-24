#ifndef VAST_ACTOR_SINK_SPAWN_H
#define VAST_ACTOR_SINK_SPAWN_H

#include <caf/actor.hpp>
#include <caf/message.hpp>

#include "vast/trial.h"

namespace vast {
namespace sink {

trial<caf::actor> spawn(caf::message const& params);

} // namespace sink
} // namespace vast

#endif
