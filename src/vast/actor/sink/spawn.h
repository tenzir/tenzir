#ifndef VAST_ACTOR_SINK_SPAWN_H
#define VAST_ACTOR_SINK_SPAWN_H

#include "vast/caf.h"
#include "vast/trial.h"

namespace vast {
namespace sink {

trial<actor> spawn(message const& params);

} // namespace sink
} // namespace vast

#endif
